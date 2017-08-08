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

import java.net.InetAddress
import java.util.Properties

import akka.actor._
import akka.event.Logging
import akka.io.IO
import akka.util.Timeout
import kamon.Kamon
import kamon.util.ConfigTools.Syntax
import org.asynchttpclient.util.ProxyUtils

import scala.concurrent.duration._
import scala.collection.JavaConverters._

object SPM extends ExtensionId[SPMExtension] with ExtensionIdProvider {
  override def createExtension(system: ExtendedActorSystem): SPMExtension = new SPMExtension(system)
  override def lookup(): ExtensionId[_ <: Extension] = SPM
}

class SPMExtension(system: ExtendedActorSystem) extends Kamon.Extension {
  implicit val s: ActorSystem = system

  val log = Logging(system, classOf[SPMExtension])

  log.info("Starting kamon-spm extension.")

  val config = system.settings.config.getConfig("kamon.spm")
  val maxQueueSize = config.getInt("max-queue-size")
  val retryInterval: FiniteDuration = config.getFiniteDuration("retry-interval")
  val sendTimeout: FiniteDuration = config.getFiniteDuration("send-timeout")
  val url = config.getString("receiver-url")
  val tracingUrl = config.getString("tracing-receiver-url")
  val token = config.getString("token")
  val traceDurationThreshold = config.getString("trace-duration-threshhold")
  val maxTraceErrorsCount = config.getString("max-trace-errors-count")

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
      proxyProps.setProperty(ProxyUtils.PROXY_USER, if (proxyUser == null) "" else  proxyUser)
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

  val hostname = if (config.hasPath("hostname-alias")) {
    config.getString("hostname-alias")
  } else {
    InetAddress.getLocalHost.getHostName
  }

  val subscriptionsConf = config.getConfig("subscriptions")
  val subscriptions = subscriptionsConf.firstLevelKeys.flatMap { category ⇒
    subscriptionsConf.getStringList(category).asScala.map { pattern ⇒
      category -> pattern
    }
  }.toList

  val sender = system.actorOf(SPMMetricsSender.props(retryInterval, Timeout(sendTimeout), maxQueueSize, url, tracingUrl, hostname, token, traceDurationThreshold.toInt, maxTraceErrorsCount.toInt, proxyProps), "spm-metrics-sender")

  val subscriber = system.actorOf(SPMMetricsSubscriber.props(sender, 50 seconds, subscriptions), "spm-metrics-subscriber")

  log.info(s"kamon-spm extension started. Hostname = ${hostname}, url = ${url}. Tracing url=${tracingUrl}")
  if (!proxyProps.isEmpty) log.info(s"You are using proxy = ${proxyProps.getProperty(ProxyUtils.PROXY_HOST)}, port = ${proxyProps.getProperty(ProxyUtils.PROXY_PORT)}.")
}
