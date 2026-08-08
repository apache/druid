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

package org.apache.druid.segment.loading;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.timeline.DataSegment;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link PartialLoadSpec} that requests every bundle the segment contains. The ceiling of a partial load,
 * as {@link PartialBaseTableLoadSpec} is its floor.
 * <p>
 * This exists so a rule can ask for the whole segment to be <em>resident</em>. Dispatching a segment without any
 * partial-load wrapper does not do that on a virtual-storage historical: the segment is registered and its bundles
 * are fetched on demand as queries touch them, which is a different thing from having them all on disk. Selecting
 * every bundle through the rule path is what actually pins the whole segment, and it announces a fingerprint like any
 * other partial load, so the coordinator can reconcile it.
 * <p>
 * Like {@link PartialBaseTableLoadSpec}, this carries no scheme-specific field — the selection is a property of the
 * segment's layout, so it is resolved on the historical by {@link #getSelectedBundleNames}.
 */
@JsonTypeName(PartialFullSegmentLoadSpec.TYPE)
public class PartialFullSegmentLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialFullSegment";

  /**
   * The fingerprint of every full-segment load. Fixed, for the same reason as
   * {@link PartialBaseTableLoadSpec#FINGERPRINT}: the selection is derived entirely from the segment's layout, so any
   * two full-segment loads of the same segment resolve identically.
   */
  public static final String FINGERPRINT = "v1:partial-full";

  /**
   * Builds the raw {@link Map} form of a {@link PartialFullSegmentLoadSpec} request. Used by the coordinator side,
   * which doesn't instantiate the typed class because doing so would require plumbing an {@link ObjectMapper} through
   * just to satisfy the constructor's lazy-delegate supplier.
   */
  public static Map<String, Object> wireForm(Map<String, Object> delegate, String fingerprint)
  {
    return Map.of(
        TYPE_FIELD, TYPE,
        DELEGATE_FIELD, delegate,
        FINGERPRINT_FIELD, fingerprint
    );
  }

  @JsonCreator
  public PartialFullSegmentLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
  }

  /**
   * Every bundle the segment carries, in container order. No layout branching is needed: whatever the writer put in
   * the file is what gets selected, so this works the same for clustered, projection-bearing, plain and legacy
   * root-only segments alike.
   */
  @Override
  public List<String> getSelectedBundleNames(DataSegment segment, SegmentFileMetadata metadata)
  {
    final Set<String> present = presentBundleNames(metadata);
    if (present.isEmpty()) {
      throw DruidException.defensive(
          "Cannot resolve full-segment bundles for segment[%s]: metadata declares no containers",
          segment.getId()
      );
    }
    return List.copyOf(present);
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
    PartialFullSegmentLoadSpec that = (PartialFullSegmentLoadSpec) o;
    return Objects.equals(getDelegate(), that.getDelegate())
        && Objects.equals(getFingerprint(), that.getFingerprint());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate(), getFingerprint());
  }

  @Override
  public String toString()
  {
    return "PartialFullSegmentLoadSpec{" +
           "delegate=" + getDelegate() +
           ", fingerprint=" + getFingerprint() +
           '}';
  }
}
