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

package org.apache.druid.indexing.SeekableStream;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.druid.segment.indexing.IOConfig;
import org.joda.time.DateTime;

import javax.annotation.Nullable;

abstract public class SeekableStreamIOConfig<T1, T2> implements IOConfig
{
  private static final boolean DEFAULT_USE_TRANSACTION = true;
  private static final boolean DEFAULT_SKIP_OFFSET_GAPS = false;

  @Nullable
  private final Integer taskGroupId;
  private final String baseSequenceName;
  private final SeekableStreamPartitions<T1, T2> startPartitions;
  private final SeekableStreamPartitions<T1, T2> endPartitions;
  private final boolean useTransaction;
  private final Optional<DateTime> minimumMessageTime;
  private final Optional<DateTime> maximumMessageTime;

  @JsonCreator
  public SeekableStreamIOConfig(
      @JsonProperty("taskGroupId") @Nullable Integer taskGroupId, // can be null for backward compabitility
      @JsonProperty("baseSequenceName") String baseSequenceName,
      @JsonProperty("startPartitions") SeekableStreamPartitions<T1, T2> startPartitions,
      @JsonProperty("endPartitions") SeekableStreamPartitions<T1, T2> endPartitions,
      @JsonProperty("useTransaction") Boolean useTransaction,
      @JsonProperty("minimumMessageTime") DateTime minimumMessageTime,
      @JsonProperty("maximumMessageTime") DateTime maximumMessageTime
  )
  {
    this.taskGroupId = taskGroupId;
    this.baseSequenceName = Preconditions.checkNotNull(baseSequenceName, "baseSequenceName");
    this.startPartitions = Preconditions.checkNotNull(startPartitions, "startPartitions");
    this.endPartitions = Preconditions.checkNotNull(endPartitions, "endPartitions");
    this.useTransaction = useTransaction != null ? useTransaction : DEFAULT_USE_TRANSACTION;
    this.minimumMessageTime = Optional.fromNullable(minimumMessageTime);
    this.maximumMessageTime = Optional.fromNullable(maximumMessageTime);

    Preconditions.checkArgument(
        startPartitions.getId().equals(endPartitions.getId()),
        "start id and end id must match"
    );

    Preconditions.checkArgument(
        startPartitions.getPartitionSequenceMap().keySet().equals(endPartitions.getPartitionSequenceMap().keySet()),
        "start partition set and end partition set must match"
    );
  }

  @Nullable
  @JsonProperty
  public Integer getTaskGroupId()
  {
    return taskGroupId;
  }

  @JsonProperty
  public String getBaseSequenceName()
  {
    return baseSequenceName;
  }

  @JsonProperty
  public SeekableStreamPartitions<T1, T2> getStartPartitions()
  {
    return startPartitions;
  }

  @JsonProperty
  public SeekableStreamPartitions<T1, T2> getEndPartitions()
  {
    return endPartitions;
  }

  @JsonProperty
  public boolean isUseTransaction()
  {
    return useTransaction;
  }

  @JsonProperty
  public Optional<DateTime> getMaximumMessageTime()
  {
    return maximumMessageTime;
  }

  @JsonProperty
  public Optional<DateTime> getMinimumMessageTime()
  {
    return minimumMessageTime;
  }

  @Override
  abstract public String toString();
}
