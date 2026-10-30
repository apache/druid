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

package org.apache.druid.indexing.kinesis.supervisor;

import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.apache.druid.indexing.seekablestream.supervisor.SupervisorIOConfigBuilder;

/**
 * Builder for {@link KinesisSupervisorIOConfig}.
 */
public class KinesisIOConfigBuilder extends SupervisorIOConfigBuilder<KinesisIOConfigBuilder, KinesisSupervisorIOConfig>
{
  private String endpoint;
  private KinesisRegion region;
  private Integer recordsPerFetch;
  private Integer fetchDelayMillis;
  private String awsAssumedRoleArn;
  private String awsExternalId;
  private Boolean deaggregate;

  public KinesisIOConfigBuilder withEndpoint(String endpoint)
  {
    this.endpoint = endpoint;
    return this;
  }

  public KinesisIOConfigBuilder withRegion(KinesisRegion region)
  {
    this.region = region;
    return this;
  }

  public KinesisIOConfigBuilder withRecordsPerFetch(Integer recordsPerFetch)
  {
    this.recordsPerFetch = recordsPerFetch;
    return this;
  }

  public KinesisIOConfigBuilder withFetchDelayMillis(Integer fetchDelayMillis)
  {
    this.fetchDelayMillis = fetchDelayMillis;
    return this;
  }

  public KinesisIOConfigBuilder withAwsAssumedRoleArn(String awsAssumedRoleArn)
  {
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    return this;
  }

  public KinesisIOConfigBuilder withAwsExternalId(String awsExternalId)
  {
    this.awsExternalId = awsExternalId;
    return this;
  }

  public KinesisIOConfigBuilder withDeaggregate(Boolean deaggregate)
  {
    this.deaggregate = deaggregate;
    return this;
  }

  /**
   * Populates this builder (base and Kinesis-specific fields) from an existing config. The
   * resolved {@code endpoint} is copied directly, so {@code region} (only used to derive an
   * endpoint) is left null.
   */
  public KinesisIOConfigBuilder copyFrom(KinesisSupervisorIOConfig io)
  {
    copyFromBase(io);
    this.endpoint = io.getEndpoint();
    this.recordsPerFetch = io.getRecordsPerFetch();
    this.fetchDelayMillis = io.getFetchDelayMillis();
    this.awsAssumedRoleArn = io.getAwsAssumedRoleArn();
    this.awsExternalId = io.getAwsExternalId();
    this.deaggregate = io.isDeaggregate();
    return this;
  }

  @Override
  public KinesisSupervisorIOConfig build()
  {
    return new KinesisSupervisorIOConfig(
        stream,
        inputFormat,
        endpoint,
        region,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestSequenceNumber,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        lateMessageRejectionStartDateTime,
        recordsPerFetch,
        fetchDelayMillis,
        awsAssumedRoleArn,
        awsExternalId,
        autoScalerConfig,
        deaggregate != null && deaggregate,
        serverPriorityToReplicas,
        boundedStreamConfig
    );
  }
}
