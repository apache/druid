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

package org.apache.druid.indexing.common.actions;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.druid.indexing.common.task.Task;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.java.util.common.JodaUtils;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * This TaskAction returns a collection of segments which have data within the specified intervals and are marked as
 * used.
 *
 * The order of segments within the returned collection is unspecified, but each segment is guaranteed to appear in
 * the collection only once.
 *
 * @implNote This action doesn't produce a {@link java.util.Set} because it's implemented via {@link
 * org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator#retrieveUsedSegmentsForIntervals} which returns
 * a collection. Producing a {@link java.util.Set} would require an unnecessary copy of segments collection.
 */
public class RetrieveUsedSegmentsAction implements TaskAction<Collection<DataSegment>>
{
  @JsonIgnore
  private final String dataSource;

  @JsonIgnore
  private final List<Interval> intervals;

  @JsonIgnore
  private final Segments visibility;

  @JsonCreator
  public RetrieveUsedSegmentsAction(
      @JsonProperty("dataSource") String dataSource,
      @Deprecated @JsonProperty("interval") Interval interval,
      @JsonProperty("intervals") Collection<Interval> intervals,
      // When JSON object is deserialized, this parameter is optional for backward compatibility.
      // Otherwise, it shouldn't be considered optional.
      @JsonProperty("visibility") @Nullable Segments visibility
  )
  {
    this.dataSource = dataSource;

    Preconditions.checkArgument(
        interval == null || intervals == null,
        "please specify intervals only"
    );

    List<Interval> theIntervals = null;
    if (interval != null) {
      theIntervals = ImmutableList.of(interval);
    } else if (intervals != null && intervals.size() > 0) {
      theIntervals = JodaUtils.condenseIntervals(intervals);
    }
    this.intervals = Preconditions.checkNotNull(theIntervals, "no intervals found");

    // Defaulting to the former behaviour when visibility wasn't explicitly specified for backward compatibility
    this.visibility = visibility != null ? visibility : Segments.ONLY_VISIBLE;
  }

  @JsonProperty
  public String getDataSource()
  {
    return dataSource;
  }

  @JsonProperty
  public List<Interval> getIntervals()
  {
    return intervals;
  }

  @JsonProperty
  public Segments getVisibility()
  {
    return visibility;
  }

  @Override
  public TypeReference<Collection<DataSegment>> getReturnTypeReference()
  {
    return new TypeReference<Collection<DataSegment>>() {};
  }

  @Override
  public Collection<DataSegment> perform(Task task, TaskActionToolbox toolbox)
  {
    return toolbox.getIndexerMetadataStorageCoordinator()
                  .retrieveUsedSegmentsForIntervals(dataSource, intervals, visibility);
  }

  @Override
  public boolean isAudited()
  {
    return false;
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

    RetrieveUsedSegmentsAction that = (RetrieveUsedSegmentsAction) o;

    if (!dataSource.equals(that.dataSource)) {
      return false;
    }
    if (!intervals.equals(that.intervals)) {
      return false;
    }
    return visibility.equals(that.visibility);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(dataSource, intervals, visibility);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "dataSource='" + dataSource + '\'' +
           ", intervals=" + intervals +
           ", visibility=" + visibility +
           '}';
  }
}
