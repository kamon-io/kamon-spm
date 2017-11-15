/*
 * =========================================================================================
 * Copyright © 2013-2015 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.spm

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.util.Properties

import akka.actor._
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.{ask, pipe}
import akka.util.Timeout
import spray.json._
import org.asynchttpclient._
import org.asynchttpclient.util.ProxyUtils

import scala.annotation.tailrec
import scala.collection.immutable.{Map, Queue}
import scala.concurrent.blocking
import scala.concurrent.duration._
import scala.concurrent.Future

class SPMMetricsSender(retryInterval: FiniteDuration, sendTimeout: Timeout, maxQueueSize: Int, url: String, tracingUrl: String, host: String, token: String, traceDurationThreshold: Int, maxTraceErrorsCount: Int, proxyProperties: Properties) extends Actor with ActorLogging {
  import context._
  import SPMMetricsSender._

  val httpClient: HttpClient = new AsyncHttpClient(sendTimeout, Logging(system, classOf[SPMMetricsSender]), proxyProperties)
  var numberOfBatchesDroppedDueToQueueSize = 0
  var numberOfRetriedBatches = 0

  implicit class ResponseSuccessFailure(resp: Response) {
    def isSuccess: Boolean =
      resp.hasResponseStatus() && resp.getStatusCode >= 200 && resp.getStatusCode <= 299
    def isFailure: Boolean = !isSuccess
  }

  private def generateQueryString(queryMap: Map[String, String]): String = {
    queryMap.map { case (key, value) ⇒ s"$key=$value" } match {
      case Nil ⇒ ""
      case xs  ⇒ s"?${xs.mkString("&")}"
    }
  }

  private def post(metrics: List[SPMMetric]): Unit = {
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$url$queryString", encodeBody(metrics)).recover {
      case t: Throwable ⇒ {
        log.error(t, "Can't post metrics.")
        ScheduleRetry
      }
    }.pipeTo(self)
  }

  private def postTraces(metrics: List[SPMMetric], segments: List[SPMMetric]): Unit = {
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$url$queryString", encodeTraceBody(metrics, segments, token)).recover {
      case t: Throwable ⇒ {
        log.error(t, "Can't post trace metrics.")
      }
    }.pipeTo(self)
  }

  private def preprocessMetrics(metrics: List[SPMMetric]): List[SPMMetric] = {
    val allFilteredMetrics = metrics.filter(metric ⇒ ((metric.category != "http-server" || metric.instrumentName.matches(".*[_]\\d\\d\\d"))))
    allFilteredMetrics
  }

  private def preprocessTraceMetrics(metrics: List[SPMMetric]): List[SPMMetric] = {
    val (errorMetrics, otherMetrics) = metrics.partition(filteredMetric ⇒ (filteredMetric.instrumentName == "errors"))
    if (errorMetrics.size > maxTraceErrorsCount) {
      log.warning("Trace error metrics were truncated")
      errorMetrics.take(maxTraceErrorsCount) ++ otherMetrics
    } else {
      metrics
    }
  }
  private def fragment(metrics: List[SPMMetric]): List[List[SPMMetric]] = {
    @tailrec
    def partition(batch: List[SPMMetric], batches: List[List[SPMMetric]]): List[List[SPMMetric]] = {
      if (batch.isEmpty) {
        batches
      } else {
        val (head, tail) = batch.splitAt(MaxMetricsPerBulk)
        partition(tail, batches :+ head)
      }
    }
    if (metrics.size < MaxMetricsPerBulk) {
      metrics :: Nil
    } else {
      partition(metrics, Nil)
    }
  }

  def receive = idle

  def idle: Receive = {
    case Send(metrics) if metrics.nonEmpty ⇒ {
      try {
        val processedMetrics = preprocessTraceMetrics(metrics.filter(metric ⇒ ((metric.category == "trace") && SPMMetric.isTraceToStore(metric, traceDurationThreshold))))
        val traceSegmentMetrics = processedMetrics.filter(metric ⇒ ((metric.category == "trace-segment")))
        if (processedMetrics.size > 0) {
          val tracingBatches = fragment(processedMetrics)
          postTraces(tracingBatches.head, traceSegmentMetrics)
        }
      } catch {
        case e: Throwable ⇒ {
          log.error(e, "Something went wrong while trace metrics sending.")
        }
      }
      try {
        val processedMetrics = preprocessMetrics(metrics)
        val batches = fragment(processedMetrics)
        post(batches.head)
        become(sending(Queue(batches: _*)))
      } catch {
        case e: Throwable ⇒ {
          log.error(e, "Something went wrong.")
        }
      }
    }
    case Send(metrics) if metrics.isEmpty ⇒ /* do nothing */
  }

  def sending(queue: Queue[List[SPMMetric]]): Receive = {
    case resp: Response if resp.isSuccess ⇒ {
      val (_, q) = queue.dequeue
      if (q.isEmpty) {
        become(idle)
      } else {
        val (metrics, _) = q.dequeue
        post(metrics)
        become(sending(q))
      }
    }
    case Send(metrics) if metrics.nonEmpty && queue.size < maxQueueSize ⇒ {
      val batches = fragment(metrics)
      become(sending(queue.enqueue(batches)))
    }
    case _: Send if queue.size >= maxQueueSize ⇒ {
      numberOfBatchesDroppedDueToQueueSize = numberOfBatchesDroppedDueToQueueSize + 1
      val batchesMessage = s"Number of dropped metrics batches: $numberOfBatchesDroppedDueToQueueSize"
      log.warning(s"Send queue is full (${queue.size}). Rejecting metrics. $batchesMessage")
    }
    case Send(metrics) if metrics.isEmpty ⇒ /* do nothing */
    case Retry ⇒ {
      val (metrics, _) = queue.dequeue
      post(metrics)
    }
    case resp: Response if resp.isFailure ⇒ {
      numberOfRetriedBatches = numberOfRetriedBatches + 1
      log.warning(s"Metrics can't be sent. Response status: ${resp.getStatusCode}. Scheduling retry. Total retries to date: $numberOfRetriedBatches")
      context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
    }
    case ScheduleRetry ⇒ {
      numberOfRetriedBatches = numberOfRetriedBatches + 1
      log.warning("Metrics can't be sent. Scheduling retry. Total retries to date: $numberOfRetriedBatches")
      context.system.scheduler.scheduleOnce(retryInterval, self, Retry)
    }
  }
}

object SPMMetricsSender {
  private case object Retry
  private case object ScheduleRetry

  case class Send(metrics: List[SPMMetric])

  def props(retryInterval: FiniteDuration, sendTimeout: Timeout, maxQueueSize: Int, url: String, tracingUrl: String, host: String, token: String, traceDurationThreshold: Int, maxTraceErrorsCount: Int,proxyProperties: Properties) =
    Props(classOf[SPMMetricsSender], retryInterval, sendTimeout, maxQueueSize, url, tracingUrl, host, token, traceDurationThreshold, maxTraceErrorsCount, proxyProperties)

  private val IndexTypeHeader = Map("index" -> Map("_type" -> "log", "_index" -> "spm-receiver"))

  private val MaxMetricsPerBulk = 100

  import spray.json.DefaultJsonProtocol._

  private def encodeBody(metrics: List[SPMMetric]): Array[Byte] = {
    val body = metrics.map { metric ⇒
      Map("body" -> SPMMetric.format(metric)).toJson
    }.toList
    (IndexTypeHeader.toJson :: body).mkString("\n").getBytes(StandardCharsets.UTF_8)
  }

  private def encodeTraceBody(metrics: List[SPMMetric], traceSegments: List[SPMMetric], token: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    metrics.foreach { metric ⇒
      val trace = SPMMetric.traceFormat(metric, token, traceSegments)
      baos.write(ByteBuffer.allocate(4).putInt(trace.size).array())
      baos.write(trace)
    }
    baos.toByteArray
  }
}

trait HttpClient {
  def post(uri: String, payload: Array[Byte]): Future[Response]
}

class AsyncHttpClient(sendTimeout: Timeout, logger: LoggingAdapter, proxyProperties: Properties) extends HttpClient {
  val cf = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(
    sendTimeout.duration.toMillis.toInt)
  if (!proxyProperties.isEmpty) {
    val proxySelector = ProxyUtils.createProxyServerSelector(proxyProperties)
    cf.setProxyServerSelector(proxySelector)
  }
  val aclient = new DefaultAsyncHttpClient(cf.build())

  import scala.concurrent.ExecutionContext.Implicits.global

  override def post(uri: String, payload: Array[Byte]) = {
    val javaFuture = aclient.preparePost(uri).setBody(payload).execute(
      new AsyncCompletionHandler[Response] {
        override def onCompleted(response: Response): Response = {
          logger.debug(s"${response.getStatusCode} ${response.getStatusText}")
          response
        }

        override def onThrowable(t: Throwable): Unit =
          logger.error(t, s"Unable to send metrics to SPM: ${t.getMessage}")
      })
    Future {
      blocking {
        javaFuture.get
      }
    }
  }

}
