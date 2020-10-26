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

package org.apache.druid.indexing.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import org.apache.druid.indexing.overlord.SegmentPublishResult;
import org.apache.druid.indexing.overlord.Segments;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.segment.realtime.appenderator.SegmentIdWithShardSpec;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.PartialShardSpec;
import org.joda.time.Interval;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class TestIndexerMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper();
  private final Set<DataSegment> published = Sets.newConcurrentHashSet();
  private final Set<DataSegment> nuked = Sets.newConcurrentHashSet();
  private final List<DataSegment> unusedSegments;

  public TestIndexerMetadataStorageCoordinator()
  {
    unusedSegments = new ArrayList<>();
  }

  @Override
  public DataSourceMetadata retrieveDataSourceMetadata(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean deleteDataSourceMetadata(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean resetDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata)
  {
    return false;
  }

  @Override
  public boolean insertDataSourceMetadata(String dataSource, DataSourceMetadata dataSourceMetadata)
  {
    return false;
  }

  @Override
  public List<DataSegment> retrieveAllUsedSegments(String dataSource, Segments visibility)
  {
    return ImmutableList.of();
  }

  @Override
  public List<Pair<DataSegment, String>> retrieveUsedSegmentsAndCreatedDates(String dataSource)
  {
    return ImmutableList.of();
  }

  @Override
  public List<DataSegment> retrieveUsedSegmentsForIntervals(
      String dataSource,
      List<Interval> intervals,
      Segments visibility
  )
  {
    return ImmutableList.of();
  }

  @Override
  public List<DataSegment> retrieveUnusedSegmentsForInterval(String dataSource, Interval interval)
  {
    synchronized (unusedSegments) {
      return ImmutableList.copyOf(unusedSegments);
    }
  }

  @Override
  public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
  {
    Set<DataSegment> added = new HashSet<>();
    for (final DataSegment segment : segments) {
      if (published.add(segment)) {
        added.add(segment);
      }
    }
    return ImmutableSet.copyOf(added);
  }

  @Override
  public SegmentPublishResult announceHistoricalSegments(
      Set<DataSegment> segments,
      DataSourceMetadata oldCommitMetadata,
      DataSourceMetadata newCommitMetadata
  )
  {
    // Don't actually compare metadata, just do it!
    return SegmentPublishResult.ok(announceHistoricalSegments(segments));
  }

  @Override
  public SegmentPublishResult commitMetadataOnly(
      String dataSource,
      DataSourceMetadata startMetadata,
      DataSourceMetadata endMetadata
  )
  {
    throw new UnsupportedOperationException("Not implemented, no test uses this currently.");
  }

  @Override
  public SegmentIdWithShardSpec allocatePendingSegment(
      String dataSource,
      String sequenceName,
      String previousSegmentId,
      Interval interval,
      PartialShardSpec partialShardSpec,
      String maxVersion,
      boolean skipSegmentLineageCheck
  )
  {
    return new SegmentIdWithShardSpec(
        dataSource,
        interval,
        maxVersion,
        partialShardSpec.complete(objectMapper, 0, 0)
    );
  }

  @Override
  public int deletePendingSegmentsCreatedInInterval(String dataSource, Interval deleteInterval)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public int deletePendingSegments(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSegments(Set<DataSegment> segments)
  {
    nuked.addAll(segments);
  }

  @Override
  public void updateSegmentMetadata(Set<DataSegment> segments)
  {
    throw new UnsupportedOperationException();
  }

  public Set<DataSegment> getPublished()
  {
    return ImmutableSet.copyOf(published);
  }

  public Set<DataSegment> getNuked()
  {
    return ImmutableSet.copyOf(nuked);
  }

  public void setUnusedSegments(List<DataSegment> newUnusedSegments)
  {
    synchronized (unusedSegments) {
      unusedSegments.clear();
      unusedSegments.addAll(newUnusedSegments);
    }
  }
}
