/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.emitter.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.kafka.clients.producer.ProducerConfig;

public class KafkaEmitterConfig {

  @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
  final private String bootstrapServers;
  @JsonProperty("metric.topic")
  final private String metricTopic;
  @JsonProperty("alert.topic")
  final private String alertTopic;
  @JsonProperty
  final private String clusterName;

  @Override
  public boolean equals(Object o) {
    if(this == o) {
      return true;
    }
    if(!(o instanceof KafkaEmitterConfig)) {
      return false;
    }

    KafkaEmitterConfig that = (KafkaEmitterConfig) o;

    if(!getBootstrapServers().equals(that.getBootstrapServers())
        || !getMetricTopic().equals(that.getMetricTopic())
        || !getAlertTopic().equals(that.getAlertTopic())
        || !getClusterName().equals(that.getClusterName())) {
      return false;
    } else {
      return true;
    }
  }
  @JsonCreator
  public KafkaEmitterConfig(@JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) String bootstrapServers,
                            @JsonProperty("metric.topic") String metricTopic,
                            @JsonProperty("alert.topic") String alertTopic,
                            @JsonProperty("clusterName") String clusterName) {
    this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers can not be null");
    this.metricTopic = Preconditions.checkNotNull(metricTopic, "metric.topic can not be null");
    this.alertTopic = Preconditions.checkNotNull(alertTopic, "alert.topic can not be null");
    this.clusterName = (clusterName == null || clusterName.isEmpty()) ? "NONAME" : clusterName;
  }

  @JsonProperty
  public String getBootstrapServers() { return bootstrapServers; }

  @JsonProperty
  public String getMetricTopic() { return metricTopic; }

  @JsonProperty
  public String getAlertTopic() { return alertTopic; }

  @JsonProperty
  public String getClusterName() { return clusterName; }

  @Override
  public String toString()
  {
    return "KafkaEmitterConfig{" +
        "bootstrap.servers='" + bootstrapServers +
        ", metric.topic=" + metricTopic +
        ", alert.topic=" + alertTopic +
        ", clusterName=" + clusterName +
        '}';
  }
}
