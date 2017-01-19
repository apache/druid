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
  @JsonProperty
  final private String topic;
  @JsonProperty("clustername")
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
        || !getTopic().equals(that.getTopic())
        || !getClusterName().equals(that.getClusterName())) {
      return false;
    } else {
      return true;
    }
  }
  @JsonCreator
  public KafkaEmitterConfig(@JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) String bootstrapServers,
                            @JsonProperty("topic") String topic, @JsonProperty("clustername") String clusterName) {
    this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers can not be null");
    this.topic = Preconditions.checkNotNull(topic, "topic can not be null");
    this.clusterName = (clusterName == null || clusterName.isEmpty()) ? "NONAME" : clusterName;
  }

  @JsonProperty
  public String getBootstrapServers() { return bootstrapServers; }

  @JsonProperty
  public String getTopic() { return topic; }

  @JsonProperty
  public String getClusterName() { return clusterName; }
}
