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
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.timeline.DataSegment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * A {@link PartialLoadSpec} that requests the segment's base table and nothing else; every row, no projections.
 * <p>
 * This is the floor of a partial load: it is what a matcher asks for when it has no scheme-specific content to
 * contribute but the segment's rows must still be resident. A projection matcher whose configured projections aren't
 * present on a segment resolves to this rather than going opaque, because a projection is always recomputable from
 * the base table, so the base table is a correct (if slower) substitute for a missing one.
 * <p>
 * This load spec no scheme-specific field; "the base table" is not a name the coordinator can resolve. Where the rows
 * actually live depends on the segment's physical layout, which only {@link #getSelectedBundleNames} can see, so the
 * spec is just the base contract and resolution happens on the historical.
 */
@JsonTypeName(PartialBaseTableLoadSpec.TYPE)
public class PartialBaseTableLoadSpec extends PartialLoadSpec
{
  public static final String TYPE = "partialBaseTable";

  /**
   * The fingerprint of every base-table load. Fixed, because the selection carries no scheme-specific content to
   * distinguish: it is derived entirely from the segment's layout, so any two base-table loads of the same segment
   * resolve identically.
   */
  public static final String FINGERPRINT = "v1:partial-base";

  /**
   * Builds the raw {@link Map} form of a {@link PartialBaseTableLoadSpec} request. Used by the coordinator-side
   * matchers, which don't instantiate the typed class because doing so would require plumbing an
   * {@link ObjectMapper} through every matcher just to satisfy the constructor's lazy-delegate supplier.
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
  public PartialBaseTableLoadSpec(
      @JsonProperty("delegate") Map<String, Object> delegate,
      @JsonProperty("fingerprint") String fingerprint,
      @JacksonInject ObjectMapper jsonMapper
  )
  {
    super(delegate, fingerprint, jsonMapper);
  }

  /**
   * Resolves "the base table" to the bundles that physically hold the segment's rows. Which bundles those are is a
   * property of the layout, not of the request:
   * <ul>
   *   <li><b>Clustered</b> — the rows are partitioned across the cluster groups, and {@code __base} holds only the
   *       parts they share, so the base table is <em>every</em> {@code __base$<clusteringValueIds>} bundle, plus
   *       {@code __base} itself when the segment carries one.</li>
   *   <li><b>Unclustered</b> — the base table is the single {@code __base} bundle.</li>
   *   <li><b>Legacy</b> — a V10 segment written before the bundle name was persisted reports every container under
   *       {@link SegmentFileBuilder#ROOT_BUNDLE_NAME}, which is then the whole segment. Gated on root being the sole
   *       bundle, matching {@code PartialSegmentBundleCacheEntry#resolveBundleName}.</li>
   * </ul>
   * Every returned name is checked against the bundles the segment actually carries first. That matters because
   * {@code resolveBundleName} deliberately passes an unknown name through unchanged so the acquire fails loudly, so
   * naming a bundle that isn't there would turn a layout the reader simply doesn't recognize into a hard load failure.
   */
  @Override
  public List<String> getSelectedBundleNames(DataSegment segment, SegmentFileMetadata metadata)
  {
    final Set<String> present = presentBundleNames(metadata);
    if (present.size() == 1 && present.contains(SegmentFileBuilder.ROOT_BUNDLE_NAME)) {
      return List.of(SegmentFileBuilder.ROOT_BUNDLE_NAME);
    }

    final List<String> clusterGroupBundles = clusterGroupBundleNames(metadata);
    if (clusterGroupBundles == null) {
      if (!present.contains(Projections.BASE_TABLE_PROJECTION_NAME)) {
        throw DruidException.defensive(
            "Cannot resolve base-table bundles for segment[%s]: no [%s] bundle among %s",
            segment.getId(),
            Projections.BASE_TABLE_PROJECTION_NAME,
            present
        );
      }
      return List.of(Projections.BASE_TABLE_PROJECTION_NAME);
    }

    final List<String> selected = new ArrayList<>(clusterGroupBundles.size() + 1);
    // The shared base bundle is optional on a clustered segment: it exists only once the segment carries shared
    // column parts. It is also an inferred dependency of every group bundle, so naming it here is belt and braces.
    if (present.contains(Projections.BASE_TABLE_PROJECTION_NAME)) {
      selected.add(Projections.BASE_TABLE_PROJECTION_NAME);
    }
    for (String groupBundle : clusterGroupBundles) {
      if (!present.contains(groupBundle)) {
        throw DruidException.defensive(
            "Cannot resolve base-table bundles for segment[%s]: metadata declares cluster-group bundle[%s] but the"
            + " segment carries %s",
            segment.getId(),
            groupBundle,
            present
        );
      }
      selected.add(groupBundle);
    }
    return selected;
  }

  /**
   * Every cluster group's bundle name when the segment's base projection is clustered, or {@code null} when it isn't.
   * Unlike {@link PartialClusterGroupLoadSpec}, this reads the group list straight out of the metadata rather than
   * indexing into it, so it needs no cross-check against the segment's tuple count.
   */
  private static List<String> clusterGroupBundleNames(SegmentFileMetadata metadata)
  {
    final List<ProjectionMetadata> projections = metadata.getProjections();
    if (projections == null || projections.isEmpty()) {
      return null;
    }
    if (!(projections.getFirst().getSchema() instanceof ClusteredValueGroupsBaseTableSchema clusteredSummary)) {
      return null;
    }
    final List<TableClusterGroupSpec> groups = clusteredSummary.getClusterGroups();
    final List<String> bundleNames = new ArrayList<>(groups.size());
    for (TableClusterGroupSpec group : groups) {
      bundleNames.add(Projections.getClusterGroupBundleName(group.getClusteringValueIds()));
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
    PartialBaseTableLoadSpec that = (PartialBaseTableLoadSpec) o;
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
    return "PartialBaseTableLoadSpec{" +
           "delegate=" + getDelegate() +
           ", fingerprint=" + getFingerprint() +
           '}';
  }
}
