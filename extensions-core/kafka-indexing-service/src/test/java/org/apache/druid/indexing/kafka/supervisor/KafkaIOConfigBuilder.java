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

package org.apache.druid.indexing.kafka.supervisor;

import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.kafkainput.KafkaInputFormat;
import org.apache.druid.indexing.seekablestream.supervisor.SupervisorIOConfigBuilder;

import java.util.Map;

/**
 * Builder for {@link KafkaSupervisorIOConfig}.
 */
public class KafkaIOConfigBuilder extends SupervisorIOConfigBuilder<KafkaIOConfigBuilder, KafkaSupervisorIOConfig>
{
  private String topic;
  private String topicPattern;
  private Map<String, Object> consumerProperties;
  private KafkaHeaderBasedFilteringConfig headerBasedFilteringConfig;

  public KafkaIOConfigBuilder withTopic(String topic)
  {
    this.topic = topic;
    return this;
  }

  public KafkaIOConfigBuilder withTopicPattern(String topicPattern)
  {
    this.topicPattern = topicPattern;
    return this;
  }

  public KafkaIOConfigBuilder withConsumerProperties(Map<String, Object> consumerProperties)
  {
    this.consumerProperties = consumerProperties;
    return this;
  }

  public KafkaIOConfigBuilder withHeaderBasedFilteringConfig(KafkaHeaderBasedFilteringConfig headerBasedFilteringConfig)
  {
    this.headerBasedFilteringConfig = headerBasedFilteringConfig;
    return this;
  }

  public KafkaIOConfigBuilder withKafkaInputFormat(InputFormat valueFormat)
  {
    this.inputFormat = new KafkaInputFormat(
        null,
        null,
        valueFormat,
        null,
        null,
        null,
        null
    );
    return this;
  }

  @Override
  public KafkaSupervisorIOConfig build()
  {
    return new KafkaSupervisorIOConfig(
        topic,
        topicPattern,
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        consumerProperties,
        autoScalerConfig,
        lagAggregator,
        null,
        startDelay,
        period,
        useEarliestSequenceNumber,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        lateMessageRejectionStartDateTime,
        null,
        headerBasedFilteringConfig,
        idleConfig,
        stopTaskCount,
        null
    );
  }
}
