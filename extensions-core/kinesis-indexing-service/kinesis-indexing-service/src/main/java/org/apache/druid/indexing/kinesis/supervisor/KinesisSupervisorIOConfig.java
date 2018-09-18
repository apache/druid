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

package org.apache.druid.indexing.kinesis.supervisor;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.indexing.kinesis.KinesisRegion;
import org.joda.time.Duration;
import org.joda.time.Period;

public class KinesisSupervisorIOConfig
{
  private final String stream;
  private final String endpoint;
  private final Integer replicas;
  private final Integer taskCount;
  private final Duration taskDuration;
  private final Duration startDelay;
  private final Duration period;
  private final boolean useEarliestSequenceNumber;
  private final Duration completionTimeout;
  private final Optional<Duration> lateMessageRejectionPeriod;
  private final Optional<Duration> earlyMessageRejectionPeriod;

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
  private final Integer fetchDelayMillis;

  private final String awsAccessKeyId;
  private final String awsSecretAccessKey;
  private final String awsAssumedRoleArn;
  private final String awsExternalId;
  private final boolean deaggregate;

  @JsonCreator
  public KinesisSupervisorIOConfig(
      @JsonProperty("stream") String stream,
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
      @JsonProperty("recordsPerFetch") Integer recordsPerFetch,
      @JsonProperty("fetchDelayMillis") Integer fetchDelayMillis,
      @JsonProperty("awsAccessKeyId") String awsAccessKeyId,
      @JsonProperty("awsSecretAccessKey") String awsSecretAccessKey,
      @JsonProperty("awsAssumedRoleArn") String awsAssumedRoleArn,
      @JsonProperty("awsExternalId") String awsExternalId,
      @JsonProperty("deaggregate") boolean deaggregate
  )
  {
    this.stream = Preconditions.checkNotNull(stream, "stream cannot be null");
    this.endpoint = endpoint != null
                    ? endpoint
                    : (region != null ? region.getEndpoint() : KinesisRegion.US_EAST_1.getEndpoint());
    this.replicas = replicas != null ? replicas : 1;
    this.taskCount = taskCount != null ? taskCount : 1;
    this.taskDuration = defaultDuration(taskDuration, "PT1H");
    this.startDelay = defaultDuration(startDelay, "PT5S");
    this.period = defaultDuration(period, "PT30S");
    this.useEarliestSequenceNumber = useEarliestSequenceNumber != null ? useEarliestSequenceNumber : false;
    this.completionTimeout = defaultDuration(completionTimeout, "PT6H");
    this.lateMessageRejectionPeriod = lateMessageRejectionPeriod == null
                                      ? Optional.<Duration>absent()
                                      : Optional.of(lateMessageRejectionPeriod.toStandardDuration());
    this.earlyMessageRejectionPeriod = earlyMessageRejectionPeriod == null
                                       ? Optional.<Duration>absent()
                                       : Optional.of(earlyMessageRejectionPeriod.toStandardDuration());
    this.recordsPerFetch = recordsPerFetch != null ? recordsPerFetch : 4000;
    this.fetchDelayMillis = fetchDelayMillis != null ? fetchDelayMillis : 0;
    this.awsAccessKeyId = awsAccessKeyId;
    this.awsSecretAccessKey = awsSecretAccessKey;
    this.awsAssumedRoleArn = awsAssumedRoleArn;
    this.awsExternalId = awsExternalId;
    this.deaggregate = deaggregate;
  }

  @JsonProperty
  public String getStream()
  {
    return stream;
  }

  @JsonProperty
  public String getEndpoint()
  {
    return endpoint;
  }

  @JsonProperty
  public Integer getReplicas()
  {
    return replicas;
  }

  @JsonProperty
  public Integer getTaskCount()
  {
    return taskCount;
  }

  @JsonProperty
  public Duration getTaskDuration()
  {
    return taskDuration;
  }

  @JsonProperty
  public Duration getStartDelay()
  {
    return startDelay;
  }

  @JsonProperty
  public Duration getPeriod()
  {
    return period;
  }

  @JsonProperty
  public boolean isUseEarliestSequenceNumber()
  {
    return useEarliestSequenceNumber;
  }

  @JsonProperty
  public Duration getCompletionTimeout()
  {
    return completionTimeout;
  }

  @JsonProperty
  public Optional<Duration> getLateMessageRejectionPeriod()
  {
    return lateMessageRejectionPeriod;
  }

  @JsonProperty
  public Optional<Duration> getEarlyMessageRejectionPeriod()
  {
    return earlyMessageRejectionPeriod;
  }

  @JsonProperty
  public Integer getRecordsPerFetch()
  {
    return recordsPerFetch;
  }

  @JsonProperty
  public Integer getFetchDelayMillis()
  {
    return fetchDelayMillis;
  }

  @JsonProperty
  public String getAwsAccessKeyId()
  {
    return awsAccessKeyId;
  }

  @JsonProperty
  public String getAwsSecretAccessKey()
  {
    return awsSecretAccessKey;
  }

  @JsonProperty
  public String getAwsAssumedRoleArn()
  {
    return awsAssumedRoleArn;
  }

  @JsonProperty
  public String getAwsExternalId()
  {
    return awsExternalId;
  }

  @JsonProperty
  public boolean isDeaggregate()
  {
    return deaggregate;
  }

  @Override
  public String toString()
  {
    return "KinesisSupervisorIOConfig{" +
           "stream='" + stream + '\'' +
           ", endpoint='" + endpoint + '\'' +
           ", replicas=" + replicas +
           ", taskCount=" + taskCount +
           ", taskDuration=" + taskDuration +
           ", startDelay=" + startDelay +
           ", period=" + period +
           ", useEarliestSequenceNumber=" + useEarliestSequenceNumber +
           ", completionTimeout=" + completionTimeout +
           ", lateMessageRejectionPeriod=" + lateMessageRejectionPeriod +
           ", earlyMessageRejectionPeriod=" + earlyMessageRejectionPeriod +
           ", recordsPerFetch=" + recordsPerFetch +
           ", fetchDelayMillis=" + fetchDelayMillis +
           ", awsAccessKeyId='" + awsAccessKeyId + '\'' +
           ", awsSecretAccessKey=" + "************************" +
           ", awsAssumedRoleArn='" + awsAssumedRoleArn + '\'' +
           ", awsExternalId='" + awsExternalId + '\'' +
           ", deaggregate=" + deaggregate +
           '}';
  }

  private static Duration defaultDuration(final Period period, final String theDefault)
  {
    return (period == null ? new Period(theDefault) : period).toStandardDuration();
  }
}
