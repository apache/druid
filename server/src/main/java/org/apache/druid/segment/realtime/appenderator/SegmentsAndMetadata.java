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
import org.apache.druid.timeline.DataSegment;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class SegmentsAndMetadata
{
  private static final SegmentsAndMetadata NIL = new SegmentsAndMetadata(Collections.emptyList(), null);

  private final Object commitMetadata;
  private final ImmutableList<DataSegment> segments;

  public SegmentsAndMetadata(
      List<DataSegment> segments,
      @Nullable Object commitMetadata
  )
  {
    this.segments = ImmutableList.copyOf(segments);
    this.commitMetadata = commitMetadata;
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

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SegmentsAndMetadata that = (SegmentsAndMetadata) o;
    return Objects.equals(commitMetadata, that.commitMetadata) &&
           Objects.equals(segments, that.segments);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(commitMetadata, segments);
  }

  @Override
  public String toString()
  {
    return "SegmentsAndMetadata{" +
           "commitMetadata=" + commitMetadata +
           ", segments=" + segments +
           '}';
  }

  public static SegmentsAndMetadata nil()
  {
    return NIL;
  }
}
