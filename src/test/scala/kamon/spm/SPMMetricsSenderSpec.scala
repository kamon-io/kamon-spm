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

import java.time.Instant

import com.typesafe.config.ConfigValueFactory
import kamon.Kamon
import kamon.metric.{Histogram, _}
import org.scalatest.{Matchers, WordSpec}

class SPMMetricsSenderSpec extends WordSpec with Matchers {

  "the SPMReporter" should {
    "process all metric types" in {
      Kamon.reconfigure(Kamon.config().withValue("kamon.spm.token", ConfigValueFactory.fromAnyRef("")))
      val reporter = new SPMReporter()
      val snapshot = new PeriodSnapshot(Instant.now, Instant.now, createMetricsSnapshot(200L))
      reporter.reportPeriodSnapshot(snapshot)
    }
  }

  def createMetricsSnapshot(value: Long) = MetricsSnapshot(
    histograms = Seq(createDistributionSnapshot(s"histogram", Map("metric" -> "histogram"), MeasurementUnit.time.microseconds, DynamicRange.Fine)(value)),
    rangeSamplers = Seq(createDistributionSnapshot(s"gauge", Map("metric" -> "gauge"), MeasurementUnit.time.microseconds, DynamicRange.Default)(value)),
    gauges = Seq(createValueSnapshot(s"gauge", Map("metric" -> "gauge"), MeasurementUnit.information.bytes, value)),
    counters = Seq(createValueSnapshot(s"counter", Map("metric" -> "counter"), MeasurementUnit.information.bytes, value))
  )

  def createValueSnapshot(metric: String, tags: Map[String, String], unit: MeasurementUnit, value: Long): MetricValue = {
    MetricValue(metric, tags, unit, value)
  }

  def createDistributionSnapshot(metric: String, tags: Map[String, String], unit: MeasurementUnit, dynamicRange: DynamicRange)(values: Long*): MetricDistribution = {
    val histogram = Kamon.histogram(metric, unit, dynamicRange).refine(tags)
    values.foreach(histogram.record)
    MetricDistribution(metric, tags, unit, dynamicRange, histogram.distribution(resetState = true))
  }

  implicit class HistogramMetricSyntax(histogram: Histogram) {
    def distribution(resetState: Boolean = true): Distribution =
      histogram match {
        case hm: HistogramMetric    => hm.refine(Map.empty[String, String]).distribution(resetState)
        case h: AtomicHdrHistogram  => h.snapshot(resetState).distribution
        case h: HdrHistogram        => h.snapshot(resetState).distribution
      }
  }
}
