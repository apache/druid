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

package org.apache.druid.emitter.kafka;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class KafkaEmitterConfig
{

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static final String TRUST_STORE_PASSWORD_KEY = "ssl.truststore.password";
  public static final String KEY_STORE_PASSWORD_KEY = "ssl.keystore.password";
  public static final String KEY_PASSWORD_KEY = "ssl.key.password";

  @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
  private final String bootstrapServers;
  @JsonProperty("metric.topic")
  private final String metricTopic;
  @JsonProperty("alert.topic")
  private final String alertTopic;
  @JsonProperty
  private final String clusterName;
  @JsonProperty("producer.config")
  private final Map<String, Object> kafkaProducerConfig;

  @JsonCreator
  public KafkaEmitterConfig(
          @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) String bootstrapServers,
          @JsonProperty("metric.topic") String metricTopic,
          @JsonProperty("alert.topic") String alertTopic,
          @JsonProperty("clusterName") String clusterName,
          @JsonProperty("producer.config") @Nullable Map<String, Object> kafkaProducerConfig
  )
  {
    this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers can not be null");
    this.metricTopic = Preconditions.checkNotNull(metricTopic, "metric.topic can not be null");
    this.alertTopic = Preconditions.checkNotNull(alertTopic, "alert.topic can not be null");
    this.clusterName = clusterName;

    Map<String, Object> properties = new HashMap<>();

    if (kafkaProducerConfig != null) {
      addProducerPropertiesFromConfig(properties, kafkaProducerConfig);
      this.kafkaProducerConfig = properties;
    } else {
      this.kafkaProducerConfig = ImmutableMap.of();
    }
  }

  @JsonProperty
  public String getBootstrapServers()
  {
    return bootstrapServers;
  }

  @JsonProperty
  public String getMetricTopic()
  {
    return metricTopic;
  }

  @JsonProperty
  public String getAlertTopic()
  {
    return alertTopic;
  }

  @JsonProperty
  public String getClusterName()
  {
    return clusterName;
  }

  @JsonProperty
  public Map<String, Object> getKafkaProducerConfig()
  {
    return kafkaProducerConfig;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    KafkaEmitterConfig that = (KafkaEmitterConfig) o;

    if (!getBootstrapServers().equals(that.getBootstrapServers())) {
      return false;
    }
    if (!getMetricTopic().equals(that.getMetricTopic())) {
      return false;
    }
    if (!getAlertTopic().equals(that.getAlertTopic())) {
      return false;
    }
    if (getClusterName() != null ? !getClusterName().equals(that.getClusterName()) : that.getClusterName() != null) {
      return false;
    }
    return getKafkaProducerConfig().equals(that.getKafkaProducerConfig());
  }

  @Override
  public int hashCode()
  {
    int result = getBootstrapServers().hashCode();
    result = 31 * result + getMetricTopic().hashCode();
    result = 31 * result + getAlertTopic().hashCode();
    result = 31 * result + (getClusterName() != null ? getClusterName().hashCode() : 0);
    result = 31 * result + getKafkaProducerConfig().hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "KafkaEmitterConfig{" +
            "bootstrap.servers='" + bootstrapServers + '\'' +
            ", metric.topic='" + metricTopic + '\'' +
            ", alert.topic='" + alertTopic + '\'' +
            ", clusterName='" + clusterName + '\'' +
            ", Producer.config=" + kafkaProducerConfig +
            '}';
  }

  public static void addProducerPropertiesFromConfig(
          Map<String, Object> properties,
          Map<String, Object> producerConfig
  )
  {
    // Extract passwords before SSL connection to Kafka
    for (Map.Entry<String, Object> entry : producerConfig.entrySet()) {
      String propertyKey = entry.getKey();
      if ((TRUST_STORE_PASSWORD_KEY).equals(propertyKey)
              || (KEY_STORE_PASSWORD_KEY).equals(propertyKey)
              || (KEY_PASSWORD_KEY).equals(propertyKey)) {
        PasswordProvider configPasswordProvider = MAPPER.convertValue(
                entry.getValue(),
                PasswordProvider.class
        );
        properties.put(propertyKey, configPasswordProvider.getPassword());
      } else {
        properties.put(propertyKey, String.valueOf(entry.getValue()));
      }
    }
  }
}
