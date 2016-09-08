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

package io.druid.indexing.test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.indexing.overlord.IndexerMetadataStorageCoordinator;
import io.druid.indexing.overlord.SegmentPublishResult;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.timeline.DataSegment;
import org.joda.time.Interval;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class TestIndexerMetadataStorageCoordinator implements IndexerMetadataStorageCoordinator
{
  final private Set<DataSegment> published = Sets.newConcurrentHashSet();
  final private Set<DataSegment> nuked = Sets.newConcurrentHashSet();
  final private List<DataSegment> unusedSegments;

  public TestIndexerMetadataStorageCoordinator()
  {
    unusedSegments = Lists.newArrayList();
  }

  @Override
  public DataSourceMetadata getDataSourceMetadata(String dataSource)
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public List<DataSegment> getUsedSegmentsForInterval(String dataSource, Interval interval) throws IOException
  {
    return ImmutableList.of();
  }

  @Override
  public List<DataSegment> getUsedSegmentsForIntervals(
      String dataSource, List<Interval> intervals
  ) throws IOException
  {
    return ImmutableList.of();
  }

  @Override
  public List<DataSegment> getUnusedSegmentsForInterval(String dataSource, Interval interval)
  {
    synchronized (unusedSegments) {
      return ImmutableList.copyOf(unusedSegments);
    }
  }

  @Override
  public Set<DataSegment> announceHistoricalSegments(Set<DataSegment> segments)
  {
    Set<DataSegment> added = Sets.newHashSet();
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
  ) throws IOException
  {
    // Don't actually compare metadata, just do it!
    return new SegmentPublishResult(announceHistoricalSegments(segments), true);
  }

  @Override
  public SegmentIdentifier allocatePendingSegment(
      String dataSource,
      String sequenceName,
      String previousSegmentId,
      Interval interval,
      String maxVersion
  ) throws IOException
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public void deleteSegments(Set<DataSegment> segments)
  {
    nuked.addAll(segments);
  }

  @Override
  public void updateSegmentMetadata(Set<DataSegment> segments) throws IOException
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
