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

package org.apache.druid.server.http;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import java.util.Objects;

/**
 * Encapsulates a {@link DataSegment} and additional metadata about it:
 * <ul>
 * <li>{@link DataSegmentPlus#used} - Boolean flag representing if the segment is used.</li>
 * <li>{@link DataSegmentPlus#createdDate} - The time when the segment was created.</li>
 * <li>{@link DataSegmentPlus#usedStatusLastUpdatedDate} - The time when the segments
 * used status was last updated.</li>
 * </ul>
 * <p>
 * This class closely resembles the row structure of the {@link MetadataStorageTablesConfig#getSegmentsTable()}.
 * </p>
 */
@UnstableApi
public class DataSegmentPlus
{
  private final DataSegment dataSegment;
  private final DateTime createdDate;
  @Nullable
  private final DateTime usedStatusLastUpdatedDate;
  private final Boolean used;

  @JsonCreator
  public DataSegmentPlus(
      @JsonProperty("dataSegment") final DataSegment dataSegment,
      @JsonProperty("createdDate") @Nullable final DateTime createdDate,
      @JsonProperty("usedStatusLastUpdatedDate") @Nullable final DateTime usedStatusLastUpdatedDate,
      @JsonProperty("used") @Nullable final Boolean used
  )
  {
    this.dataSegment = dataSegment;
    this.createdDate = createdDate;
    this.usedStatusLastUpdatedDate = usedStatusLastUpdatedDate;
    this.used = used;
  }

  @Nullable
  @JsonProperty
  public DateTime getCreatedDate()
  {
    return createdDate;
  }

  @Nullable
  @JsonProperty
  public DateTime getUsedStatusLastUpdatedDate()
  {
    return usedStatusLastUpdatedDate;
  }

  @JsonProperty
  public DataSegment getDataSegment()
  {
    return dataSegment;
  }

  @Nullable
  @JsonProperty
  public Boolean getUsed()
  {
    return used;
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
    DataSegmentPlus that = (DataSegmentPlus) o;
    return Objects.equals(dataSegment, that.getDataSegment())
           && Objects.equals(createdDate, that.getCreatedDate())
           && Objects.equals(usedStatusLastUpdatedDate, that.getUsedStatusLastUpdatedDate())
           && Objects.equals(used, that.getUsed());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(
        dataSegment,
        createdDate,
        usedStatusLastUpdatedDate,
        used
    );
  }

  @Override
  public String toString()
  {
    return "DataSegmentPlus{" +
           "createdDate=" + getCreatedDate() +
           ", usedStatusLastUpdatedDate=" + getUsedStatusLastUpdatedDate() +
           ", dataSegment=" + getDataSegment() +
           ", used=" + getUsed() +
           '}';
  }
}
