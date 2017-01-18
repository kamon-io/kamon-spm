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

import java.nio.charset.StandardCharsets

import scala.concurrent.Await
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.concurrent.Promise
import scala.concurrent.duration._
import scala.util.Success

import org.asynchttpclient.{ HttpResponseStatus, Response }
import org.mockito.Mockito._
import org.scalatest.concurrent.Eventually.eventually
import org.scalatest.mockito.MockitoSugar
import org.scalatest.time.SpanSugar

import akka.actor.Props
import akka.testkit.TestActorRef
import akka.util.Timeout
import kamon.Kamon
import kamon.metric.instrument.Time
import kamon.spm.SPMMetricsSender.Send
import kamon.testkit.BaseKamonSpec
import kamon.util.MilliTimestamp

class SPMMetricsSenderSpec extends BaseKamonSpec("spm-metrics-sender-spec") with MockitoSugar with SpanSugar {

  private def testMetrics(prefix: String = ""): List[SPMMetric] = {
    (0 until 2).map { i ⇒
      val histo = Kamon.metrics.histogram(s"histo-$i")
      histo.record(1)
      SPMMetric(new MilliTimestamp(123L), "histogram", s"${prefix}-entry-$i", s"histo-$i", Map(), Time.Milliseconds, histo.collect(collectionContext))
    }.toList
  }

  case class RequestData(uri: String, payload: Array[Byte])

  class MockHttpClient(httpResponseCode: Int = 200) extends HttpClient {
    val requests = new java.util.concurrent.ArrayBlockingQueue[RequestData](100)

    override def post(uri: String, payload: Array[Byte]): Future[Response] = {
      requests.add(RequestData(uri, payload))
      response
    }

    def dequeueRequest() = Future {
      requests.take()
    }

    def response = Future {
      val response = mock[Response]
      when(response.getStatusCode).thenReturn(httpResponseCode)
      when(response.hasResponseStatus()).thenReturn(true)
      response
    }
  }

  class MockedSPMMetricsSender(mockHttpClient: MockHttpClient, retryInterval: FiniteDuration, sendTimeout: Timeout, maxQueueSize: Int, url: String, tracingUrl: String, host: String,
      token: String, traceDurationThreshold: Int, maxTraceErrorsCount: Int) extends SPMMetricsSender(retryInterval, sendTimeout, maxQueueSize,
      url, tracingUrl, host, token, traceDurationThreshold, maxTraceErrorsCount) {
    override val httpClient = mockHttpClient
  }

  "spm metrics sender" should {
    "send metrics to receiver" in {
      val sender = TestActorRef(Props(new MockedSPMMetricsSender(new MockHttpClient, 5 seconds, Timeout(5 seconds), 100, "http://localhost:1234/api", "http://localhost:1234/trace", "host-1", "1234", 10, 10)))
      sender ! Send(testMetrics())

      val client = sender.underlyingActor.asInstanceOf[MockedSPMMetricsSender].httpClient
      val request = Await.result(client.dequeueRequest(), 2 seconds)
      request.uri shouldEqual "http://localhost:1234/api?host=host-1&token=1234"

      val payload = new String(request.payload, StandardCharsets.UTF_8)
      payload.split("\n") should have length 3
    }

    "resend metrics in case of exception or failure response status" in {
      val mockHttpClient = new MockHttpClient(404)
      val sender = TestActorRef(Props(new MockedSPMMetricsSender(mockHttpClient, 2 seconds, Timeout(5 seconds), 100, "http://localhost:1234/api", "http://localhost:1234/trace", "host-1", "1234", 10, 10)))
      sender ! Send(testMetrics())

      val senderActor =  sender.underlyingActor.asInstanceOf[MockedSPMMetricsSender]
      eventually(timeout(5 seconds), interval(50 millis)) {
        senderActor.numberOfRetriedBatches should atLeast(1)
      }

    }

    "ignore new metrics in case when send queue is full" in {
      val mockHttpClient = new MockHttpClient {
        override def response = Promise[Response].future
      }
      val sender = TestActorRef(Props(new MockedSPMMetricsSender(mockHttpClient, 2 seconds, Timeout(5 seconds), 5, "http://localhost:1234/api", "http://localhost:1234/trace", "host-1", "1234", 10, 10)))

      (0 until 5).foreach(_ ⇒ sender ! Send(testMetrics()))

      sender ! Send(testMetrics())

      val senderActor =  sender.underlyingActor.asInstanceOf[MockedSPMMetricsSender]
      eventually(timeout(5 seconds), interval(50 millis)) {
        senderActor.numberOfBatchesDroppedDueToQueueSize shouldEqual 1
      }

    }
  }
}
