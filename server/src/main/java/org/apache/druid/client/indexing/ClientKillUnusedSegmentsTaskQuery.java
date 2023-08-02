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

package org.apache.druid.client.indexing;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.joda.time.Interval;

import javax.annotation.Nullable;

import java.util.Objects;

/**
 * Client representation of org.apache.druid.indexing.common.task.KillUnusedSegmentsTask. JSON searialization
 * fields of this class must correspond to those of org.apache.druid.indexing.common.task.KillUnusedSegmentsTask, except
 * for "id" and "context" fields.
 */
public class ClientKillUnusedSegmentsTaskQuery implements ClientTaskQuery
{
  public static final String TYPE = "kill";

  private final String id;
  private final String dataSource;
  private final Interval interval;
  private final Boolean markAsUnused;
  @Nullable private final Integer limit;

  @JsonCreator
  public ClientKillUnusedSegmentsTaskQuery(
      @JsonProperty("id") String id,
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval,
      @JsonProperty("markAsUnused") @Deprecated Boolean markAsUnused,
      @JsonProperty("limit") Integer limit
  )
  {
    this.id = Preconditions.checkNotNull(id, "id");
    this.dataSource = dataSource;
    this.interval = interval;
    this.markAsUnused = markAsUnused;
    if (null != limit) {
      Preconditions.checkArgument(limit > 0, "limit must be > 0");
    }
    this.limit = limit;
  }

  @JsonProperty
  @Override
  public String getId()
  {
    return id;
  }

  @JsonProperty
  @Override
  public String getType()
  {
    return TYPE;
  }

  @JsonProperty
  @Override
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public Interval getInterval()
  {
    return interval;
  }

  @JsonProperty
  public Boolean getMarkAsUnused()
  {
    return markAsUnused;
  }

  @JsonProperty
  @Nullable
  public Integer getLimit()
  {
    return limit;
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
    ClientKillUnusedSegmentsTaskQuery that = (ClientKillUnusedSegmentsTaskQuery) o;
    return Objects.equals(id, that.id)
           && Objects.equals(dataSource, that.dataSource)
           && Objects.equals(interval, that.interval)
           && Objects.equals(markAsUnused, that.markAsUnused)
           && Objects.equals(limit, that.limit);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(id, dataSource, interval, markAsUnused, limit);
  }
}
