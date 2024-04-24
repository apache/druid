/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.emitter.prometheus.metrics;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.emitter.prometheus.PrometheusEmitterConfig;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.SortedSet;

@JsonTypeName(MetricType.DimensionMapNames.SUMMARY)
public class Summary extends Metric<io.prometheus.client.Summary>
{
  private final List<Double> quantiles;
  private final List<Double> errors;
  private final Long ageSeconds;
  private final Integer ageBuckets;

  public Summary(
      @JsonProperty("dimensions") SortedSet<String> dimensions,
      @JsonProperty("type") MetricType type,
      @JsonProperty("help") String help,
      @JsonProperty("quantiles") List<Double> quantiles,
      @JsonProperty("errors") List<Double> errors,
      @JsonProperty("ageSeconds") @Nullable Long ageSeconds,
      @JsonProperty("ageBuckets") @Nullable Integer ageBuckets
  )
  {
    super(dimensions, type, help);
    this.quantiles = quantiles;
    this.errors = errors;
    this.ageBuckets = ageBuckets;
    this.ageSeconds = ageSeconds;
  }

  @Override
  public void record(String[] labelValues, double value)
  {
    this.getCollector().labels(labelValues).observe(value);
  }

  @Override
  public void createCollector(String name, PrometheusEmitterConfig emitterConfig)
  {
    super.configure(name, emitterConfig);
    io.prometheus.client.Summary.Builder builder = io.prometheus.client.Summary.build(getFormattedName(), help);
    Iterator<Double> quantileIterator = quantiles.iterator();
    Iterator<Double> errorIterator = errors.iterator();
    while (quantileIterator.hasNext() && errorIterator.hasNext()) {
      builder = builder.quantile(quantileIterator.next(), errorIterator.next());
    }
    if (Objects.nonNull(ageSeconds)) {
      builder = builder.maxAgeSeconds(ageSeconds);
    }
    if (Objects.nonNull(ageBuckets)) {
      builder = builder.ageBuckets(ageBuckets);
    }
    this.setCollector(
        builder
            .namespace(emitterConfig.getNamespace())
            .labelNames(this.getDimensions())
            .register()
    );
  }
}
