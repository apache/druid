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
import com.google.common.base.Preconditions;
import org.apache.druid.error.DruidException;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.timeline.ClusterGroupTuples;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A {@link PartialLoadSpec} that requests partial loading of a clustered segment's cluster groups. The base class
 * carries the common {@code fingerprint} and {@code delegate} wire fields; this subtype adds the resolved
 * {@code clusterGroupIndices} (positions into {@link org.apache.druid.timeline.ClusterGroupTuples#tuples()}) that
 * the historical should range-read into the local segment.
 */
@JsonTypeName(PartialClusterGroupLoadSpec.TYPE)
public class PartialClusterGroupLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialClusterGroup";

  /**
   * Builds the raw wire-form {@link Map} representation of a {@link PartialClusterGroupLoadSpec} request. Used by the
   * coordinator-side matcher (which doesn't instantiate the typed class because doing so would require plumbing an
   * {@link ObjectMapper} through every matcher just to satisfy the constructor's lazy-delegate supplier).
   */
  public static Map<String, Object> wireForm(
      Map<String, Object> delegate,
      List<Integer> clusterGroupIndices,
      String fingerprint
  )
  {
    return Map.of(
        "type", TYPE,
        "delegate", delegate,
        "clusterGroupIndices", clusterGroupIndices,
        "fingerprint", fingerprint
    );
  }

  private final List<Integer> clusterGroupIndices;

  @JsonCreator
  public PartialClusterGroupLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("clusterGroupIndices") List<Integer> clusterGroupIndices,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
    // An empty index list is used when a cluster-group matcher applies to a (clustered) segment but no configured
    // pattern matches its cluster groups. The historical-side partial loader honors this by loading nothing.
    Preconditions.checkNotNull(clusterGroupIndices, "clusterGroupIndices");
    this.clusterGroupIndices = List.copyOf(clusterGroupIndices);
  }

  @JsonProperty
  public List<Integer> getClusterGroupIndices()
  {
    return clusterGroupIndices;
  }

  /**
   * Resolve each {@code clusterGroupIndex} against the segment's {@link ClusteredValueGroupsBaseTableSchema} to a
   * {@code __base$<clusteringValueIds>} bundle name via {@link Projections#getClusterGroupBundleName}. The base
   * projection carries the authoritative group list; each index picks the group at that position and its
   * {@code clusteringValueIds} disambiguate the bundle in the V10 layout.
   * <p>
   * Defensive tripwires fire on any structural inconsistency: empty projections list, non-clustered base projection,
   * segment's cluster-group tuple count vs. metadata's group count mismatch, or index out of range. These conditions
   * are unreachable under a healthy writer/reader contract; a throw here indicates a coding bug.
   * <p>
   * Returns an empty list when {@code clusterGroupIndices} is empty (the "sibling-empty" case where a matcher
   * applied to a clustered segment but no configured pattern matched any group).
   */
  @Override
  public List<String> getSelectedBundleNames(DataSegment segment, SegmentFileMetadata metadata)
  {
    if (clusterGroupIndices.isEmpty()) {
      return List.of();
    }
    final List<ProjectionMetadata> projections = metadata.getProjections();
    if (projections == null || projections.isEmpty()) {
      throw DruidException.defensive(
          "Cannot resolve cluster-group bundle names for segment[%s]: metadata has no projections",
          segment.getId()
      );
    }
    if (!(projections.getFirst().getSchema() instanceof ClusteredValueGroupsBaseTableSchema clusteredSummary)) {
      throw DruidException.defensive(
          "Cannot resolve cluster-group bundle names for segment[%s]: base projection is not clustered",
          segment.getId()
      );
    }
    final List<TableClusterGroupSpec> metadataGroups = clusteredSummary.getClusterGroups();
    final ClusterGroupTuples segmentTuples = segment.getClusterGroups();
    final int segmentTupleCount = segmentTuples == null ? 0 : segmentTuples.tuples().size();
    if (segmentTupleCount != metadataGroups.size()) {
      throw DruidException.defensive(
          "Cluster-group count mismatch for segment[%s]: DataSegment has [%s] tuples, metadata has [%s] groups",
          segment.getId(),
          segmentTupleCount,
          metadataGroups.size()
      );
    }
    final List<String> bundleNames = new ArrayList<>(clusterGroupIndices.size());
    for (int idx : clusterGroupIndices) {
      if (idx < 0 || idx >= metadataGroups.size()) {
        throw DruidException.defensive(
            "Cluster-group index [%s] is out of range [0, %s) for segment[%s]",
            idx,
            metadataGroups.size(),
            segment.getId()
        );
      }
      bundleNames.add(Projections.getClusterGroupBundleName(metadataGroups.get(idx).getClusteringValueIds()));
    }
    return bundleNames;
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
    PartialClusterGroupLoadSpec that = (PartialClusterGroupLoadSpec) o;
    return Objects.equals(getDelegate(), that.getDelegate())
        && Objects.equals(clusterGroupIndices, that.clusterGroupIndices)
        && Objects.equals(getFingerprint(), that.getFingerprint());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate(), clusterGroupIndices, getFingerprint());
  }

  @Override
  public String toString()
  {
    return "PartialClusterGroupLoadSpec{" +
           "delegate=" + getDelegate() +
           ", clusterGroupIndices=" + clusterGroupIndices +
           ", fingerprint=" + getFingerprint() +
           '}';
  }
}
