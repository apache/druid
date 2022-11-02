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
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaEmitterConfig
{
  public enum EventType
  {
    METRICS,
    ALERTS,
    REQUESTS,
    SEGMENTMETADATA {
      @Override
      public String toString()
      {
        return "segmentMetadata";
      }
    };

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static EventType fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  public static final Set<EventType> DEFAULT_EVENT_TYPES = ImmutableSet.of(EventType.ALERTS, EventType.METRICS);

  public enum SegmentMetadataTopicFormat
  {
    JSON,
    PROTOBUF;

    @JsonValue
    @Override
    public String toString()
    {
      return StringUtils.toLowerCase(this.name());
    }

    @JsonCreator
    public static SegmentMetadataTopicFormat fromString(String name)
    {
      return valueOf(StringUtils.toUpperCase(name));
    }
  }

  @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG)
  private final String bootstrapServers;
  @Nullable @JsonProperty("event.types")
  private final Set<EventType> eventTypes;
  @Nullable @JsonProperty("metric.topic")
  private final String metricTopic;
  @Nullable @JsonProperty("alert.topic")
  private final String alertTopic;
  @Nullable @JsonProperty("request.topic")
  private final String requestTopic;
  @Nullable @JsonProperty("segmentMetadata.topic")
  private final String segmentMetadataTopic;
  @Nullable @JsonProperty("segmentMetadata.topic.format")
  private final SegmentMetadataTopicFormat segmentMetadataTopicFormat;
  @JsonProperty
  private final String clusterName;
  @JsonProperty("producer.config")
  private final Map<String, String> kafkaProducerConfig;

  @JsonCreator
  public KafkaEmitterConfig(
      @JsonProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) String bootstrapServers,
      @Nullable @JsonProperty("event.types") Set<EventType> eventTypes,
      @Nullable @JsonProperty("metric.topic") String metricTopic,
      @Nullable @JsonProperty("alert.topic") String alertTopic,
      @Nullable @JsonProperty("request.topic") String requestTopic,
      @Nullable @JsonProperty("segmentMetadata.topic") String segmentMetadataTopic,
      @Nullable @JsonProperty("segmentMetadata.topic.format") SegmentMetadataTopicFormat segmentMetadataTopicFormat,
      @JsonProperty("clusterName") String clusterName,
      @JsonProperty("producer.config") @Nullable Map<String, String> kafkaProducerConfig
  )
  {
    this.bootstrapServers = Preconditions.checkNotNull(bootstrapServers, "bootstrap.servers can not be null");
    this.eventTypes = validateEventTypes(eventTypes, requestTopic);
    this.segmentMetadataTopicFormat = segmentMetadataTopicFormat == null ? SegmentMetadataTopicFormat.JSON : segmentMetadataTopicFormat;

    this.metricTopic = this.eventTypes.contains(EventType.METRICS) ? Preconditions.checkNotNull(metricTopic, "metric.topic can not be null") : null;
    this.alertTopic = this.eventTypes.contains(EventType.ALERTS) ? Preconditions.checkNotNull(alertTopic, "alert.topic can not be null") : null;
    this.requestTopic = this.eventTypes.contains(EventType.REQUESTS) ? Preconditions.checkNotNull(requestTopic, "request.topic can not be null") : null;
    this.segmentMetadataTopic = this.eventTypes.contains(EventType.SEGMENTMETADATA) ? Preconditions.checkNotNull(segmentMetadataTopic, "segmentMetadata.topic can not be null") : null;
    this.clusterName = clusterName;
    this.kafkaProducerConfig = kafkaProducerConfig == null ? ImmutableMap.of() : kafkaProducerConfig;
  }

  private Set<EventType> validateEventTypes(Set<EventType> eventTypes, String requestTopic)
  {
    // Unless explicitly overridden, kafka emitter will always emit metrics and alerts
    if (eventTypes == null) {
      Set<EventType> defaultEventTypes = new HashSet<>(DEFAULT_EVENT_TYPES);
      // To maintain backwards compatibility, if eventTypes is not set, then requests are sent out or not
      // based on the `request.topic` config
      if (requestTopic != null) {
        defaultEventTypes.add(EventType.REQUESTS);
      }
      return defaultEventTypes;
    }
    return eventTypes;
  }

  @JsonProperty
  public String getBootstrapServers()
  {
    return bootstrapServers;
  }

  @JsonProperty
  public Set<EventType> getEventTypes()
  {
    return eventTypes;
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

  @Nullable
  public String getRequestTopic()
  {
    return requestTopic;
  }

  @Nullable
  public String getSegmentMetadataTopic()
  {
    return segmentMetadataTopic;
  }

  @JsonProperty
  public SegmentMetadataTopicFormat getSegmentMetadataTopicFormat()
  {
    return segmentMetadataTopicFormat;
  }

  @JsonProperty
  public Map<String, String> getKafkaProducerConfig()
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

    if (getEventTypes() != null ? !getEventTypes().equals(that.getEventTypes()) : that.getEventTypes() != null) {
      return false;
    }

    if (getMetricTopic() != null ? !getMetricTopic().equals(that.getMetricTopic()) : that.getMetricTopic() != null) {
      return false;
    }

    if (getAlertTopic() != null ? !getAlertTopic().equals(that.getAlertTopic()) : that.getAlertTopic() != null) {
      return false;
    }

    if (getRequestTopic() != null ? !getRequestTopic().equals(that.getRequestTopic()) : that.getRequestTopic() != null) {
      return false;
    }

    if (getSegmentMetadataTopic() != null ? !getSegmentMetadataTopic().equals(that.getSegmentMetadataTopic()) : that.getSegmentMetadataTopic() != null) {
      return false;
    }

    if (getSegmentMetadataTopicFormat() != null ? !getSegmentMetadataTopicFormat().equals(that.getSegmentMetadataTopicFormat()) : that.getSegmentMetadataTopicFormat() != null) {
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
    result = 31 * result + (getEventTypes() != null ? getEventTypes().hashCode() : 0);
    result = 31 * result + (getMetricTopic() != null ? getMetricTopic().hashCode() : 0);
    result = 31 * result + (getAlertTopic() != null ? getAlertTopic().hashCode() : 0);
    result = 31 * result + (getRequestTopic() != null ? getRequestTopic().hashCode() : 0);
    result = 31 * result + (getSegmentMetadataTopic() != null ? getSegmentMetadataTopic().hashCode() : 0);
    result = 31 * result + (getSegmentMetadataTopicFormat() != null ? getSegmentMetadataTopicFormat().hashCode() : 0);
    result = 31 * result + (getClusterName() != null ? getClusterName().hashCode() : 0);
    result = 31 * result + getKafkaProducerConfig().hashCode();
    return result;
  }

  @Override
  public String toString()
  {
    return "KafkaEmitterConfig{" +
           "bootstrap.servers='" + bootstrapServers + '\'' +
           ", event.types='" + eventTypes.toString() + '\'' +
           ", metric.topic='" + metricTopic + '\'' +
           ", alert.topic='" + alertTopic + '\'' +
           ", request.topic='" + requestTopic + '\'' +
           ", segmentMetadata.topic='" + segmentMetadataTopic + '\'' +
           ", segmentMetadata.topic.format='" + segmentMetadataTopicFormat + '\'' +
           ", clusterName='" + clusterName + '\'' +
           ", Producer.config=" + kafkaProducerConfig +
           '}';
  }
}
