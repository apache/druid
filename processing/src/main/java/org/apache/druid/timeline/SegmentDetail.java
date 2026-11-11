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

package org.apache.druid.timeline;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.EnumSet;

/**
 * The optional top-level fields of a {@link DataSegment}: the ones that a segment can be returned without, so that a
 * caller that does not need them can avoid paying for them in serialization size and heap.
 * <p>
 * The rest of {@link DataSegment} is never optional: {@link DataSegment#getId()} (and therefore the dataSource,
 * interval, and version), {@link DataSegment#getShardSpec()}, {@link DataSegment#getBinaryVersion()}, and
 * {@link DataSegment#getSize()} are always populated.
 * <p>
 * Use {@link DataSegment#retainOnlyDetails} to drop the details that are not wanted.
 *
 * @see org.apache.druid.indexing.common.actions.RetrieveUsedSegmentsAction
 */
public enum SegmentDetail
{
  DIMENSIONS("dimensions"),
  METRICS("metrics"),
  PROJECTIONS("projections"),
  CLUSTER_GROUPS("clusterGroups"),
  COMPACTION_STATE("lastCompactionState"),
  LOAD_SPEC("loadSpec"),
  ROW_COUNT("totalRows"),
  INDEXING_STATE_FINGERPRINT("indexingStateFingerprint");

  private final String jsonName;

  SegmentDetail(final String jsonName)
  {
    this.jsonName = jsonName;
  }

  /**
   * All details.
   */
  public static EnumSet<SegmentDetail> all()
  {
    return EnumSet.allOf(SegmentDetail.class);
  }

  /**
   * No details.
   */
  public static EnumSet<SegmentDetail> none()
  {
    return EnumSet.noneOf(SegmentDetail.class);
  }

  /**
   * Parses a collection of names, skipping any name that this version of Druid does not recognize.
   */
  @Nullable
  public static EnumSet<SegmentDetail> fromNamesLenient(@Nullable final Collection<String> names)
  {
    if (names == null) {
      return null;
    }

    final EnumSet<SegmentDetail> retVal = none();
    for (final String name : names) {
      final SegmentDetail detail = fromNameLenient(name);
      if (detail != null) {
        retVal.add(detail);
      }
    }
    return retVal;
  }

  /**
   * Returns the {@link SegmentDetail} for a name, or throws {@link IllegalArgumentException} if none exists.
   */
  @JsonCreator
  public static SegmentDetail fromName(final String name)
  {
    final SegmentDetail detail = fromNameLenient(name);
    if (detail == null) {
      throw new IAE("No such SegmentDetail[%s]", name);
    }
    return detail;
  }

  /**
   * Returns the {@link SegmentDetail} for a name, or null if none exists.
   */
  @Nullable
  public static SegmentDetail fromNameLenient(@Nullable final String name)
  {
    if (name == null) {
      return null;
    }
    for (final SegmentDetail detail : values()) {
      if (detail.jsonName.equalsIgnoreCase(name)) {
        return detail;
      }
    }
    return null;
  }

  @Override
  @JsonValue
  public String toString()
  {
    return jsonName;
  }
}
