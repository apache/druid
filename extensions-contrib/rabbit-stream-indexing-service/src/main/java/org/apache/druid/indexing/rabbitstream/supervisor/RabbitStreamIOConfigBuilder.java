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

package org.apache.druid.indexing.rabbitstream.supervisor;

import org.apache.druid.indexing.seekablestream.supervisor.SupervisorIOConfigBuilder;

import java.util.Map;

/**
 * Builder for {@link RabbitStreamSupervisorIOConfig}.
 */
public class RabbitStreamIOConfigBuilder
    extends SupervisorIOConfigBuilder<RabbitStreamIOConfigBuilder, RabbitStreamSupervisorIOConfig>
{
  private String uri;
  private Map<String, Object> consumerProperties;
  private Long pollTimeout;

  public RabbitStreamIOConfigBuilder withUri(String uri)
  {
    this.uri = uri;
    return this;
  }

  public RabbitStreamIOConfigBuilder withConsumerProperties(Map<String, Object> consumerProperties)
  {
    this.consumerProperties = consumerProperties;
    return this;
  }

  public RabbitStreamIOConfigBuilder withPollTimeout(Long pollTimeout)
  {
    this.pollTimeout = pollTimeout;
    return this;
  }

  /**
   * Populates this builder (base and Rabbit-specific fields) from an existing config.
   */
  public RabbitStreamIOConfigBuilder copyFrom(RabbitStreamSupervisorIOConfig io)
  {
    copyFromBase(io);
    this.uri = io.getUri();
    this.consumerProperties = io.getConsumerProperties();
    this.pollTimeout = io.getPollTimeout();
    return this;
  }

  @Override
  public RabbitStreamSupervisorIOConfig build()
  {
    return new RabbitStreamSupervisorIOConfig(
        stream,
        uri,
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        consumerProperties,
        autoScalerConfig,
        pollTimeout,
        startDelay,
        period,
        completionTimeout,
        useEarliestSequenceNumber,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        lateMessageRejectionStartDateTime,
        stopTaskCount,
        serverPriorityToReplicas,
        boundedStreamConfig
    );
  }
}
