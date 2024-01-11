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
import lombok.Getter;
import org.apache.druid.emitter.prometheus.PrometheusEmitterConfig;

import java.util.SortedSet;

@JsonTypeName(MetricType.DimensionMapNames.HISTOGRAM)
public class Histogram extends Metric<io.prometheus.client.Histogram> {
    private final double[] buckets;
    public Histogram(
            @JsonProperty("dimensions") SortedSet<String> dimensions,
            @JsonProperty("type") MetricType type,
            @JsonProperty("help") String help,
            @JsonProperty("buckets") double[] buckets
    ) {
        super(dimensions, type, help);
        this.buckets = buckets;
    }

    @Override
    public void record(String[] labelValues, double value) {
        this.getCollector().labels(labelValues).observe(value);
    }

    @Override
    public void createCollector(String name, PrometheusEmitterConfig emitterConfig) {
        super.configure(name, emitterConfig);
        this.setCollector(
                new io.prometheus.client.Histogram.Builder()
                        .namespace(this.getNamespace())
                        .name(this.getFormattedName())
                        .labelNames(this.getDimensions())
                        .buckets(this.buckets)
                        .help(help)
                        .register()
        );
    }
}
