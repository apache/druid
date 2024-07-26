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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.indexing.kinesis.KinesisIndexTaskIOConfig;
import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.apache.druid.indexing.seekablestream.supervisor.IdleConfig;
import org.apache.druid.indexing.seekablestream.supervisor.SeekableStreamSupervisorIOConfig;
import org.apache.druid.indexing.seekablestream.supervisor.autoscaler.AutoScalerConfig;
import org.joda.time.DateTime;
import org.joda.time.Period;

import javax.annotation.Nullable;

public class KinesisSupervisorIOConfig extends SeekableStreamSupervisorIOConfig
{
  private final String endpoint;

  // In determining a suitable value for recordsPerFetch:
  //   - Each data record can be up to 1 MB in size
  //   - Each shard can read up to 2 MB per second
  //   - The maximum size of data that GetRecords can return is 10 MB. If a call returns this amount of data,
  //     subsequent calls made within the next 5 seconds throw ProvisionedThroughputExceededException.
  //
  // If there is insufficient provisioned throughput on the shard, subsequent calls made within the next 1 second
  // throw ProvisionedThroughputExceededException. Note that GetRecords won't return any data when it throws an
  // exception. For this reason, we recommend that you wait one second between calls to GetRecords; however, it's
  // possible that the application will get exceptions for longer than 1 second.
  private final Integer recordsPerFetch;
  private final int fetchDelayMillis;

  private final String awsAssumedRoleArn;
  private final String awsExternalId;
  private final boolean deaggregate;

  @JsonCreator
  public KinesisSupervisorIOConfig(
      @JsonProperty("stream") String stream,
      @JsonProperty("inputFormat") InputFormat inputFormat,
      @JsonProperty("endpoint") String endpoint,
      @JsonProperty("region") KinesisRegion region,
      @JsonProperty("replicas") Integer replicas,
      @JsonProperty("taskCount") Integer taskCount,
      @JsonProperty("taskDuration") Period taskDuration,
      @JsonProperty("startDelay") Period startDelay,
      @JsonProperty("period") Period period,
      @JsonProperty("useEarliestSequenceNumber") Boolean useEarliestSequenceNumber,
      @JsonProperty("completionTimeout") Period completionTimeout,
      @JsonProperty("lateMessageRejectionPeriod") Period lateMessageRejectionPeriod,
      @JsonProperty("earlyMessageRejectionPeriod") Period earlyMessageRejectionPeriod,
      @JsonProperty("lateMessageRejectionStartDateTime") DateTime lateMessageRejectionStartDateTime,
      @JsonProperty("recordsPerFetch") @Deprecated Integer recordsPerFetch,
      @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
      @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") String awsExternalId,
      @Nullable @JsonProperty("autoScalerConfig") AutoScalerConfig autoScalerConfig,
      @JsonProperty("deaggregate") @Deprecated boolean deaggregate
  )
  {
    super(
        Preconditions.checkNotNull(stream, "stream"),
        inputFormat,
        replicas,
        taskCount,
        taskDuration,
        startDelay,
        period,
        useEarliestSequenceNumber,
        completionTimeout,
        lateMessageRejectionPeriod,
        earlyMessageRejectionPeriod,
        autoScalerConfig,
        lateMessageRejectionStartDateTime,
        new IdleConfig(null, null),
        null
    );

    this.endpoint = endpoint != null
                    ? endpoint
                    : (region != null ? region.getEndpoint() : KinesisRegion.US_EAST_1.getEndpoint());
    this.recordsPerFetch = recordsPerFetch;
    this.fetchDelayMillis = fetchDelayMillis != null
                            ? fetchDelayMillis
                            : KinesisIndexTaskIOConfig.DEFAULT_FETCH_DELAY_MILLIS;
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.awsExternalId = awsExternalId;
    this.deaggregate = deaggregate;
  }

  @JsonProperty
  public String getEndpoint()
  {
    return endpoint;
  }

  @Nullable
  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public Integer getRecordsPerFetch()
  {
    return recordsPerFetch;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public int getFetchDelayMillis()
  {
    return fetchDelayMillis;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAwsAssumedRoleArn()
  {
    return awsAssumedRoleArn;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getAwsExternalId()
  {
    return awsExternalId;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_DEFAULT)
  public boolean isDeaggregate()
  {
    return deaggregate;
  }

  @Override
  public String toString()
  {
    return "KinesisSupervisorIOConfig{" +
           "stream='" + getStream() + '\'' +
           ", endpoint='" + endpoint + '\'' +
           ", replicas=" + getReplicas() +
           ", taskCount=" + getTaskCount() +
           ", autoScalerConfig=" + getAutoScalerConfig() +
           ", taskDuration=" + getTaskDuration() +
           ", startDelay=" + getStartDelay() +
           ", period=" + getPeriod() +
           ", useEarliestSequenceNumber=" + isUseEarliestSequenceNumber() +
           ", completionTimeout=" + getCompletionTimeout() +
           ", lateMessageRejectionPeriod=" + getLateMessageRejectionPeriod() +
           ", earlyMessageRejectionPeriod=" + getEarlyMessageRejectionPeriod() +
           ", lateMessageRejectionStartDateTime=" + getLateMessageRejectionStartDateTime() +
           ", recordsPerFetch=" + recordsPerFetch +
           ", fetchDelayMillis=" + fetchDelayMillis +
           ", awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", awsExternalId='" + awsExternalId + '\'' +
           ", deaggregate=" + deaggregate +
           '}';
  }
}
