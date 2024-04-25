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

package org.apache.druid.segment.realtime.appenderator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.segment.SegmentSchemaMapping;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

public class SegmentsAndCommitMetadata
{
  private static final SegmentsAndCommitMetadata NIL
      = new SegmentsAndCommitMetadata(Collections.emptyList(), null, null, null);

  private final Object commitMetadata;
  private final ImmutableList<DataSegment> segments;
  private final SegmentSchemaMapping segmentSchemaMapping;

  private final ImmutableSet<DataSegment> upgradedSegments;

  public SegmentsAndCommitMetadata(
      List<DataSegment> segments,
      Object commitMetadata
  )
  {
    this(segments, commitMetadata, null, null);
  }

  public SegmentsAndCommitMetadata(
      List<DataSegment> segments,
      Object commitMetadata,
      SegmentSchemaMapping segmentSchemaMapping
  )
  {
    this(segments, commitMetadata, segmentSchemaMapping, null);
  }

  public SegmentsAndCommitMetadata(
      List<DataSegment> segments,
      @Nullable Object commitMetadata,
      @Nullable SegmentSchemaMapping segmentSchemaMapping,
      @Nullable Set<DataSegment> upgradedSegments
  )
  {
    this.segments = ImmutableList.copyOf(segments);
    this.commitMetadata = commitMetadata;
    this.upgradedSegments = upgradedSegments == null ? null : ImmutableSet.copyOf(upgradedSegments);
    this.segmentSchemaMapping = segmentSchemaMapping;
  }

  public SegmentsAndCommitMetadata withUpgradedSegments(Set<DataSegment> upgradedSegments)
  {
    return new SegmentsAndCommitMetadata(
        this.segments,
        this.commitMetadata,
        this.segmentSchemaMapping,
        upgradedSegments
    );
  }

  @Nullable
  public Object getCommitMetadata()
  {
    return commitMetadata;
  }

  public List<DataSegment> getSegments()
  {
    return segments;
  }

  /**
   * @return the set of extra upgraded segments committed due to a concurrent replace.
   */
  @Nullable
  public Set<DataSegment> getUpgradedSegments()
  {
    return upgradedSegments;
  }

  public SegmentSchemaMapping getSegmentSchemaMapping()
  {
    return segmentSchemaMapping;
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
    SegmentsAndCommitMetadata that = (SegmentsAndCommitMetadata) o;
    return Objects.equals(commitMetadata, that.commitMetadata) &&
           Objects.equals(upgradedSegments, that.upgradedSegments) &&
           Objects.equals(segmentSchemaMapping, that.segmentSchemaMapping) &&
           Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(commitMetadata, segments, upgradedSegments, segmentSchemaMapping);
  }

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{" +
           "commitMetadata=" + commitMetadata +
           ", segments=" + SegmentUtils.commaSeparatedIdentifiers(segments) +
           ", upgradedSegments=" + SegmentUtils.commaSeparatedIdentifiers(upgradedSegments) +
           ", segmentSchemaMapping=" + segmentSchemaMapping +
           '}';
  }

  public static SegmentsAndCommitMetadata nil()
  {
    return NIL;
  }
}
