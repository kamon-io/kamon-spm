/*
 * =========================================================================================
 * Copyright © 2013-2018 the kamon project <http://kamon.io/>
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 * =========================================================================================
 */

package kamon.spm

import java.io.ByteArrayOutputStream
import java.net.InetAddress
import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.text.{DecimalFormat, DecimalFormatSymbols}
import java.time.Duration
import java.util
import java.util.{Locale, Properties}

import com.typesafe.config.Config
import kamon.metric.MeasurementUnit.Dimension.Information
import kamon.metric.MeasurementUnit.{information, time}
import kamon.metric.{MeasurementUnit, _}
import kamon.{Kamon, MetricReporter}
import org.asynchttpclient.util.ProxyUtils
import org.asynchttpclient._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.Map
import scala.concurrent.{Future, blocking}
import spray.json._
import DefaultJsonProtocol._
import com.sematext.spm.client.tracing.thrift._

import scala.concurrent._
import ExecutionContext.Implicits.global
import scala.util.Random


class SPMReporter extends MetricReporter {
  private val log = LoggerFactory.getLogger(classOf[SPMReporter])
  val config = Kamon.config().getConfig("kamon.spm")
  val maxQueueSize = config.getInt("max-queue-size")
  val retryInterval = config.getDuration("retry-interval")
  val url = config.getString("receiver-url")
  val tracingUrl = config.getString("tracing-receiver-url")
  val token = config.getString("token")
  val customMarker = config.getString("custom-metric-marker")
  val traceDurationThreshold = config.getString("trace-duration-threshhold").toLong
  private val IndexTypeHeader = Map("index" -> Map("_type" -> "log", "_index" -> "spm-receiver"))
  val host = if (config.hasPath("hostname-alias")) {
    config.getString("hostname-alias")
  } else {
    InetAddress.getLocalHost.getHostName
  }
  var httpClient: AsyncHttpClient = null;

  override def start(): Unit = {
    log.info("Starting kamon-spm reporter")
    val sendTimeout = config.getDuration("send-timeout")
    val proxy = System.getProperty("http.proxyHost")
    val proxyProps = new Properties()
    if (proxy != null && !proxy.isEmpty) {
      val proxyPort = System.getProperty("http.proxyPort")
      if (proxyPort == null || proxyPort.isEmpty) {
        log.error(s"Proxy port not specified")
      } else {
        proxyProps.setProperty(ProxyUtils.PROXY_HOST, proxy)
        proxyProps.setProperty(ProxyUtils.PROXY_PORT, proxyPort)
        val proxyUser = System.getProperty("http.proxyUser")
        val proxyPassword = System.getProperty("http.proxyPassword")
        proxyProps.setProperty(ProxyUtils.PROXY_USER, if (proxyUser == null) "" else proxyUser)
        proxyProps.setProperty(ProxyUtils.PROXY_PASSWORD, if (proxyPassword == null) "" else proxyPassword)
      }
    } else {
      val proxy = config.getString("proxy-server")
      if (proxy != null && !proxy.isEmpty) {
        proxyProps.setProperty(ProxyUtils.PROXY_HOST, proxy)
        proxyProps.setProperty(ProxyUtils.PROXY_PORT, config.getInt("proxy-port").toString)
        proxyProps.setProperty(ProxyUtils.PROXY_USER, config.getString("proxy-user"))
        proxyProps.setProperty(ProxyUtils.PROXY_PASSWORD, config.getString("proxy-password"))
      }
    }
    httpClient = new AsyncHttpClient(sendTimeout.toMillis.toInt, log, proxyProps)
  }

  override def stop(): Unit = {
    if (httpClient != null) {
      httpClient.close()
    }
  }

  override def reconfigure(config: Config): Unit = {
  }


  override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
    val metrics = snapshot.metrics
    try {
      postTraces(buildTraceRequestBody(snapshot))
    } catch {
      case e: Throwable ⇒ {
        log.error("Something went wrong while trace metrics sending.", e)
      }
    }
    try {
      post(buildRequestBody(snapshot))
    } catch {
      case e: Throwable ⇒ {
        log.error("Something went wrong.", e)
      }
    }
  }

  private def buildTraceRequestBody(snapshot: PeriodSnapshot): Array[Byte] = {
    val timestamp: Long = (snapshot.from.toEpochMilli)
    val baos = new ByteArrayOutputStream()
    val histograms = (snapshot.metrics.histograms.filter(metric ⇒ (metric.name == "span.processing-time" && metric.distribution.max > traceDurationThreshold))).foreach { metric ⇒
      val event = new TTracingEvent()
      val thrift = new TPartialTransaction()
      val rnd = new Random()
      val callId = rnd.nextLong()
      thrift.setParentCallId(0)
      thrift.setCallId(callId)
      thrift.setTraceId(rnd.nextLong())
      val calls = new java.util.ArrayList[TCall]()
      val tCall = new TCall()
      tCall.setDuration(convert(metric.unit, metric.distribution.max))
      tCall.setStartTimestamp(timestamp)
      tCall.setEndTimestamp(timestamp + convert(metric.unit, metric.distribution.max))
      tCall.setCallId(callId)
      tCall.setParentCallId(0)
      tCall.setSignature(metric.tags("operation"))
      calls.add(tCall)
      //TODO: implement trace sections
      thrift.setRequest(metric.tags("operation"))
      thrift.setStartTimestamp(timestamp)
      thrift.setEndTimestamp(timestamp + convert(metric.unit, metric.distribution.max))
      thrift.setDuration(convert(metric.unit, metric.distribution.max))
      thrift.setToken(token)
      thrift.setFailed(metric.tags("error").toBoolean)
      thrift.setEntryPoint(true)
      thrift.setAsynchronous(false)
      thrift.setTransactionType(TTransactionType.WEB)
      val summary = new TWebTransactionSummary()
      summary.setRequest(metric.tags("operation"))
      thrift.setTransactionSummary(ThriftUtils.binaryProtocolSerializer().serialize(summary))
      val endpoint = new TEndpoint()
      endpoint.setHostname(InetAddress.getLocalHost.getHostName)
      endpoint.setAddress(InetAddress.getLocalHost.getHostAddress)
      thrift.setEndpoint(endpoint)
      thrift.setCalls(calls)
      thrift.setParameters(new java.util.HashMap[String, String]())
      event.setPartialTransaction(thrift)
      event.eventType = TTracingEventType.PARTIAL_TRANSACTION
      val trace = ThriftUtils.binaryProtocolSerializer().serialize(event)
      baos.write(ByteBuffer.allocate(4).putInt(trace.size).array())
      baos.write(trace)
    }
    baos.toByteArray
  }

  private def buildRequestBody(snapshot: PeriodSnapshot): Array[Byte] = {
    val timestamp: Long = (snapshot.from.toEpochMilli)

    val histograms = (snapshot.metrics.histograms ++ snapshot.metrics.rangeSamplers).map { metric ⇒
      Map("body" -> {
        val min = convert(metric.unit, metric.distribution.min)
        val max = convert(metric.unit, metric.distribution.max)
        val sum = convert(metric.unit, metric.distribution.sum)
        val p50 = convert(metric.unit, metric.distribution.percentile(50D).value)
        val p90 = convert(metric.unit, metric.distribution.percentile(90D).value)
        val p95 = convert(metric.unit, metric.distribution.percentile(95D).value)
        val p99 = convert(metric.unit, metric.distribution.percentile(99D).value)
        val p995 = convert(metric.unit, metric.distribution.percentile(99.5D).value)
        s"${prefix(metric, timestamp)}\t${min}\t${max}\t${sum}\t${metric.distribution.count}\t${p50}\t${p90}\t${p95}\t${p99}\t${p995}"
      }
      ).toJson
    }.toList

    val counters = (snapshot.metrics.counters).map { metric ⇒
      Map("body" -> {
        metric.name match {
          case "host.file-system.activity" => getTagOrEmptyString(metric.tags, "operation") match {
            case "read" => s"${timestamp}\t${"system-metric-file-system-reads"}\t${timestamp}\tfile-system\t0\t0\t${metric.value}\t0"
            case "write" => s"${timestamp}\t${"system-metric-file-system-writes"}\t${timestamp}\tfile-system\t0\t0\t${metric.value}\t0"
            case _ => defaultMetricString(timestamp, metric.name)
          }
          case "executor.tasks" => getTagOrEmptyString(metric.tags, "state") match {
            case "completed" => s"${timestamp}\t${"akka-dispatcher-processed-tasks"}\t${timestamp}\t\t0\t0\t${metric.value}\t0"
            case _ => defaultMetricString(timestamp, metric.name)
          }
          case "host.network.packets" => {
            if (metric.tags.contains("state")) {
              getTagOrEmptyString(metric.tags, "state") match {
                case "error" => {
                  getTagOrEmptyString(metric.tags, "direction") match {
                    case "transmitted" => s"${timestamp}\t${"system-metric-tx-errors"}\t${timestamp}\t0\t0\t0\t0\t${metric.value}"
                    case "received" => s"${timestamp}\t${"system-metric-rx-errors"}\t${timestamp}\t0\t0\t0\t0\t${metric.value}"
                    case _ => defaultMetricString(timestamp, metric.name)
                  }
                }
                case "dropped" => {
                  getTagOrEmptyString(metric.tags, "direction") match {
                    case "transmitted" => s"${timestamp}\t${"system-metric-tx-dropped"}\t${timestamp}\t0\t0\t0\t0\t${metric.value}"
                    case "received" => s"${timestamp}\t${"system-metric-rx-dropped"}\t${timestamp}\t0\t0\t0\t0\t${metric.value}"
                    case _ => defaultMetricString(timestamp, metric.name)
                  }
                }
                case _ => defaultMetricString(timestamp, metric.name)
              }
            } else {
              getTagOrEmptyString(metric.tags, "direction") match {
                case "transmitted" => s"${timestamp}\t${"system-metric-tx-bytes"}\t${timestamp}\t0\t0\t0\t${metric.value}\t0"
                case "received" => s"${timestamp}\t${"system-metric-rx-bytes"}\t${timestamp}\t0\t0\t0\t${metric.value}\t0"
                case _ => defaultMetricString(timestamp, metric.name)
              }
            }
          }
          case _ => {
            if (metric.tags.contains(customMarker)) {
              s"${timestamp}\t${"counter-counter"}\t${timestamp}\t${metric.name}\t${metric.value}"
            } else {
              s"${prefix(metric, timestamp)}\t${metric.value}"
            }
          }
        }
      }
      ).toJson
    }.toList

    val gauges = (snapshot.metrics.gauges).map { metric ⇒
      Map("body" -> {
        metric.name match {
          case "executor.pool" => getTagOrEmptyString(metric.tags, "setting") match {
            case "parallelism" => s"${timestamp}\t${"akka-dispatcher-parallelism"}\t${timestamp}\t0\t0\t${metric.value}\t0\t0"
            case "min" => s"${timestamp}\t${"akka-dispatcher-min-pool-size"}\t${timestamp}\t${metric.value}\t0\t0\t0\t0"
            case "max" => s"${timestamp}\t${"akka-dispatcher-max-pool-size"}\t${timestamp}\t0\t${metric.value}\t0\t0\t0"
            case "corePoolSize" => s"${timestamp}\t${"akka-dispatcher-core-pool-size"}\t${timestamp}\t${metric.value}\t0\t0\t0\t0"
            case _ => defaultMetricString(timestamp, metric.name)
          }
          case "jvm.class-loading" => {
            getTagOrEmptyString(metric.tags, "mode") match {
              case "currently-loaded" => s"${timestamp}\t${"system-metric-classes-currently-loaded"}\t${timestamp}\t\t0\t${metric.value}\t0\t0"
              case "unloaded" => s"${timestamp}\t${"system-metric-classes-unloaded"}\t${timestamp}\t\t0\t${metric.value}\t0\t0"
              case "loaded" => s"${timestamp}\t${"system-metric-classes-loaded"}\t${timestamp}\t\t0\t${metric.value}\t0\t0"
              case _ => defaultMetricString(timestamp, metric.name)
            }
          }
          case "jvm.threads" => getTagOrEmptyString(metric.tags, "measure") match {
            case "daemon" => s"${timestamp}\t${"system-metric-daemon-thread-count"}\t${timestamp}\t\t0\t0\t${metric.value}\t1"
            case "peak" => s"${timestamp}\t${"system-metric-peak-thread-count"}\t${timestamp}\t\t0\t0\t${metric.value}\t1"
            case "total" => s"${timestamp}\t${"system-metric-thread-count"}\t${timestamp}\t\t0\t0\t${metric.value}\t1"
            case _ => defaultMetricString(timestamp, metric.name)
          }
          case _ => {
            if (metric.tags.contains(customMarker)) {
              s"${timestamp}\t${"gauge-gauge"}\t${timestamp}\t${metric.name}\t0\t0\t${metric.value}\t1"
            } else {
              s"${prefix(metric, timestamp)}\t${metric.value}"
            }
          }
        }
      }
      ).toJson
    }.toList
    (IndexTypeHeader.toJson :: histograms ::: counters ::: gauges).mkString("\n").getBytes(StandardCharsets.UTF_8)
  }

  private def prefix(metric: MetricDistribution, timestamp: Long): String = {
    metric.name match {
      case "host.cpu" => s"${timestamp}\t${"system-metric-cpu-" + getTagOrEmptyString(metric.tags, "mode")}\t${timestamp}\t"
      case "host.load-average" => {
        getTagOrEmptyString(metric.tags, "period") match {
          case "1" => s"${timestamp}\t${"system-metric-one-minute"}\t${timestamp}\t"
          case "5" => s"${timestamp}\t${"system-metric-five-minutes"}\t${timestamp}\t"
          case "15" => s"${timestamp}\t${"system-metric-fifteen-minutes"}\t${timestamp}\t"
          case _ => defaultMetricString(timestamp, metric.name)
        }
      }
      case "host.swap" => s"${timestamp}\t${"system-metric-swap-" + getTagOrEmptyString(metric.tags, "mode")}\t${timestamp}\t"
      case "host.memory" => s"${timestamp}\t${"system-metric-memory-" + getTagOrEmptyString(metric.tags, "mode")}\t${timestamp}\t"
      case "akka.actor.time-in-mailbox" => s"${timestamp}\t${"akka-actor-time-in-mailbox"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "path")}"
      case "akka.actor.processing-time" => s"${timestamp}\t${"akka-actor-processing-time"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "path")}"
      case "akka.actor.mailbox-size" => s"${timestamp}\t${"akka-actor-mailbox-size"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "path")}"
      case "executor.queue" => s"${timestamp}\t${"akka-dispatcher-queued-tasks-count"}\t${timestamp}\t"
      case "executor.threads" => {
        getTagOrEmptyString(metric.tags, "state") match {
          case "total" => s"${timestamp}\t${"akka-dispatcher-running-threads"}\t${timestamp}\t"
          case "active" => s"${timestamp}\t${"akka-dispatcher-active-threads"}\t${timestamp}\t"
          case _ => defaultMetricString(timestamp, metric.name)
        }
      }
      case "akka.router.routing-time" => s"${timestamp}\t${"akka-router-routing-time"}\t${timestamp}\t"
      case "akka.router.time-in-mailbox" => s"${timestamp}\t${"akka-router-time-in-mailbox"}\t${timestamp}\t"
      case "akka.router.processing-time" => s"${timestamp}\t${"akka-router-processing-time"}\t${timestamp}\t"
      case "jvm.gc" => s"${timestamp}\t${"system-metric-garbage-collection-time"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "collector")}"
      case "jvm.gc.promotion" => s"${timestamp}\t${"system-metric-garbage-collection-count"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "space")}"
      case "jvm.memory" => s"${timestamp}\t${"system-metric-" + getTagOrEmptyString(metric.tags, "segment") + "-" + getTagOrEmptyString(metric.tags, "measure")}\t${timestamp}\t"
      case "span.processing-time" => {
        getTagOrEmptyString(metric.tags, "error") match {
          case "false" => s"${timestamp}\t${"trace-elapsed-time"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "operation")}"
          case "true" => s"${timestamp}\t${"trace-errors"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "operation")}"
          case _ => defaultMetricString(timestamp, metric.name)
        }
      }
      case "process.cpu" => {
        getTagOrEmptyString(metric.tags, "mode") match {
          case "system" => s"${timestamp}\t${"system-metric-process-system-cpu"}\t${timestamp}\t"
          case "total" => s"${timestamp}\t${"system-metric-process-cpu"}\t${timestamp}\t"
          case "user" => s"${timestamp}\t${"system-metric-process-user-cpu"}\t${timestamp}\t"
          case _ => defaultMetricString(timestamp, metric.name)
        }
      }
      case _ => {
        if (metric.tags.contains(customMarker)) {
          s"${timestamp}\t${"histogram-histogram"}\t${timestamp}\t${metric.name}"
        } else {
          defaultMetricString(timestamp, metric.name)
        }
      }

    }
  }

  private def prefix(metric: MetricValue, timestamp: Long): String = {
    metric.name match {
      case "host.file-system.activity" => getTagOrEmptyString(metric.tags, "operation") match {
        case "read" => s"${timestamp}\t${"system-metric-file-system-reads"}\t${timestamp}\t"
        case "write" => s"${timestamp}\t${"system-metric-file-system-writes"}\t${timestamp}\t"
        case _ => defaultMetricString(timestamp, metric.name)
      }
      case "akka.actor.errors" => s"${timestamp}\t${"akka-actor-errors"}\t${timestamp}\t${getTagOrEmptyString(metric.tags, "path")}"
      case "akka.router.errors" => s"${timestamp}\t${"akka-router-errors"}\t${timestamp}\t"
      case _ => defaultMetricString(timestamp, metric.name)
    }
  }

  private def convert(unit: MeasurementUnit, value: Long): Long = unit.dimension match {
    case MeasurementUnit.time ⇒ MeasurementUnit.scale(value, unit, time.milliseconds).toLong
    case Information ⇒ MeasurementUnit.scale(value, unit, information.bytes).toLong
    case _ ⇒ value
  }

  trait HttpClient {
    def post(uri: String, payload: Array[Byte]): Future[Response]
    def close()
  }

  class AsyncHttpClient(sendTimeout: Int, logger: Logger, proxyProperties: Properties) extends HttpClient {
    val cf = new DefaultAsyncHttpClientConfig.Builder().setRequestTimeout(sendTimeout)
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
            logger.error(s"Unable to send metrics to SPM: ${t.getMessage}", t)
        })
      Future {
        blocking {
          javaFuture.get
        }
      }
    }
    override def close() = {
      aclient.close()
    }

  }

  private def generateQueryString(queryMap: Map[String, String]): String = {
    queryMap.map { case (key, value) ⇒ s"$key=$value" } match {
      case Nil ⇒ ""
      case xs ⇒ s"?${xs.mkString("&")}"
    }
  }

  private def post(body: Array[Byte]): Unit = {
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$url$queryString", body).recover {
      case t: Throwable ⇒ {
        log.error("Can't post metrics.", t)
      }
    }
  }

  private def postTraces(body: Array[Byte]): Unit = {
    if (body.length == 0) return;
    val queryString = generateQueryString(Map("host" -> host, "token" -> token))
    httpClient.post(s"$tracingUrl$queryString", body).recover {
      case t: Throwable ⇒ {
        log.error("Can't post trace metrics.", t)
      }
    }
  }

  private def getTagOrEmptyString(tags: Map[String, String], tagname: String): String = {
    if (tags.contains(tagname)) {
      tags.get(tagname).get
    } else {
      ""
    }
  }

  private def defaultMetricString(timestamp: Long, name: String): String = {
    s"${timestamp}\t${name.replaceAll("\\.", "-")}\t${timestamp}\t"
  }
}

