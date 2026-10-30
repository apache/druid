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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.OrderBy;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileContainerMetadata;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.ClusteredValueGroupsBaseTableSchema;
import org.apache.druid.segment.projections.ClusteringDictionaries;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.projections.TableClusterGroupSpec;
import org.apache.druid.segment.projections.TableProjectionSchema;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class PartialBaseTableLoadSpecTest
{
  private static final Map<String, Object> DELEGATE = ImmutableMap.of(
      "type", "stub",
      "path", "/var/druid/segments/foo"
  );

  private static ObjectMapper configuredMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.registerSubtypes(PartialBaseTableLoadSpec.class, StubLoadSpec.class);
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    PartialBaseTableLoadSpec spec = new PartialBaseTableLoadSpec(
        DELEGATE,
        PartialBaseTableLoadSpec.FINGERPRINT,
        jsonMapper
    );
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(PartialBaseTableLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialBaseTableType() throws Exception
  {
    PartialBaseTableLoadSpec spec = new PartialBaseTableLoadSpec(
        DELEGATE,
        PartialBaseTableLoadSpec.FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialBaseTable", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(PartialBaseTableLoadSpec.FINGERPRINT, wireForm.get("fingerprint"));
    // No scheme-specific field: "the base table" is resolved from layout on the historical.
    Assertions.assertEquals(3, wireForm.size());
  }

  @Test
  void testFingerprintIsNotTheEmptyLoadSentinel()
  {
    // A base-table load puts every row on the historical; an empty load puts nothing there. Sharing a fingerprint
    // would make a rule swap between the two invisible to the coordinator, and the load would never be re-issued.
    Assertions.assertNotEquals("v1:partial-empty", PartialBaseTableLoadSpec.FINGERPRINT);
  }

  @Test
  void testGetSelectedBundleNamesUnclusteredSegmentSelectsBase()
  {
    final SegmentFileMetadata metadata = metadata(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME, "user_hourly"),
        List.of(unclusteredBaseProjection(), projection("user_hourly"))
    );
    Assertions.assertEquals(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesSegmentWithNoProjectionsSelectsBase()
  {
    final SegmentFileMetadata metadata = metadata(List.of(Projections.BASE_TABLE_PROJECTION_NAME), null);
    Assertions.assertEquals(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesClusteredSegmentSelectsEveryGroup()
  {
    // On a clustered segment the rows are spread across the group bundles, so "the base table" is all of them, plus
    // the shared __base bundle when the segment carries one.
    final List<TableClusterGroupSpec> groups = List.of(
        new TableClusterGroupSpec(List.of(0), 10),
        new TableClusterGroupSpec(List.of(1), 20),
        new TableClusterGroupSpec(List.of(2), 30)
    );
    final List<String> groupBundles = groups.stream()
                                            .map(g -> Projections.getClusterGroupBundleName(g.getClusteringValueIds()))
                                            .toList();
    final List<String> present = new ArrayList<>();
    present.add(Projections.BASE_TABLE_PROJECTION_NAME);
    present.addAll(groupBundles);
    final SegmentFileMetadata metadata = metadata(present, List.of(clusteredBaseProjection(groups)));

    final List<String> expected = new ArrayList<>();
    expected.add(Projections.BASE_TABLE_PROJECTION_NAME);
    expected.addAll(groupBundles);
    Assertions.assertEquals(expected, spec().getSelectedBundleNames(anySegment(), metadata));
  }

  @Test
  void testGetSelectedBundleNamesClusteredSegmentWithoutSharedBaseBundle()
  {
    // __base is optional on a clustered segment: it only exists once there are shared column parts to put in it.
    final List<TableClusterGroupSpec> groups = List.of(
        new TableClusterGroupSpec(List.of(0), 10),
        new TableClusterGroupSpec(List.of(1), 20)
    );
    final List<String> groupBundles = groups.stream()
                                            .map(g -> Projections.getClusterGroupBundleName(g.getClusteringValueIds()))
                                            .toList();
    final SegmentFileMetadata metadata = metadata(groupBundles, List.of(clusteredBaseProjection(groups)));
    Assertions.assertEquals(groupBundles, spec().getSelectedBundleNames(anySegment(), metadata));
  }

  @Test
  void testGetSelectedBundleNamesClusteredSegmentExcludesProjectionBundles()
  {
    // The base table is the rows, not the precomputation over them.
    final List<TableClusterGroupSpec> groups = List.of(new TableClusterGroupSpec(List.of(0), 10));
    final String groupBundle = Projections.getClusterGroupBundleName(List.of(0));
    final SegmentFileMetadata metadata = metadata(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME, groupBundle, "user_hourly"),
        List.of(clusteredBaseProjection(groups), projection("user_hourly"))
    );
    Assertions.assertEquals(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME, groupBundle),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesLegacyRootOnlySegment()
  {
    // A V10 segment written before the bundle name was persisted reports everything under __root__, which is then the
    // whole segment. Gated on root being the sole bundle, matching PartialSegmentBundleCacheEntry#resolveBundleName.
    final SegmentFileMetadata metadata = metadata(List.of(SegmentFileBuilder.ROOT_BUNDLE_NAME), null);
    Assertions.assertEquals(
        List.of(SegmentFileBuilder.ROOT_BUNDLE_NAME),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesThrowsWhenNoBaseBundlePresent()
  {
    // Named bundles but no __base and no clustering: the reader would fail loudly on the acquire anyway, so say so
    // here where the message can name the cause.
    final SegmentFileMetadata metadata = metadata(List.of("user_hourly"), List.of(projection("user_hourly")));
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec().getSelectedBundleNames(anySegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("no [__base] bundle among"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testGetSelectedBundleNamesThrowsWhenDeclaredGroupBundleMissing()
  {
    // Metadata declares two cluster groups but the segment only carries one group bundle: writer/reader drift.
    final List<TableClusterGroupSpec> groups = List.of(
        new TableClusterGroupSpec(List.of(0), 10),
        new TableClusterGroupSpec(List.of(1), 20)
    );
    final SegmentFileMetadata metadata = metadata(
        List.of(Projections.getClusterGroupBundleName(List.of(0))),
        List.of(clusteredBaseProjection(groups))
    );
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec().getSelectedBundleNames(anySegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("metadata declares cluster-group bundle"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testLoadSegmentDelegatesToInner() throws Exception
  {
    StubLoadSpec.LOAD_CALLS.set(0);
    LoadSpec.LoadSpecResult result = spec().loadSegment(new File("/tmp/dest"));
    Assertions.assertEquals(1, StubLoadSpec.LOAD_CALLS.get());
    Assertions.assertEquals(42L, result.getSize());
  }

  @Test
  void testOpenRangeReaderDelegatesToInner() throws Exception
  {
    StubLoadSpec.RANGE_CALLS.set(0);
    SegmentRangeReader reader = spec().openRangeReader();
    Assertions.assertNotNull(reader);
    Assertions.assertEquals(1, StubLoadSpec.RANGE_CALLS.get());
  }

  @Test
  void testOpenRangeReaderReturnsNullWhenInnerDoesNotSupport() throws Exception
  {
    PartialBaseTableLoadSpec spec = new PartialBaseTableLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        PartialBaseTableLoadSpec.FINGERPRINT,
        jsonMapper
    );
    Assertions.assertNull(spec.openRangeReader());
  }

  @Test
  void testRejectsNullDelegate()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialBaseTableLoadSpec(null, PartialBaseTableLoadSpec.FINGERPRINT, jsonMapper)
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialBaseTableLoadSpec(DELEGATE, null, jsonMapper)
    );
  }

  private PartialBaseTableLoadSpec spec()
  {
    return new PartialBaseTableLoadSpec(DELEGATE, PartialBaseTableLoadSpec.FINGERPRINT, jsonMapper);
  }

  private static final RowSignature CLUSTERING_TENANT = RowSignature.builder()
                                                                    .add("tenant", ColumnType.STRING)
                                                                    .build();

  /**
   * Metadata carrying one container per entry of {@code bundleNames} — the base-table spec only reads which bundles
   * exist, not what is in them — plus the given projection list.
   */
  private static SegmentFileMetadata metadata(
      List<String> bundleNames,
      @Nullable List<ProjectionMetadata> projections
  )
  {
    final List<SegmentFileContainerMetadata> containers = new ArrayList<>(bundleNames.size());
    long offset = 0;
    for (String bundleName : bundleNames) {
      containers.add(new SegmentFileContainerMetadata(offset, 100L, bundleName));
      offset += 100L;
    }
    return new SegmentFileMetadata(containers, Map.of(), null, null, null, projections, null);
  }

  private static ProjectionMetadata unclusteredBaseProjection()
  {
    return new ProjectionMetadata(
        100,
        new TableProjectionSchema(
            VirtualColumns.EMPTY,
            List.of(ColumnHolder.TIME_COLUMN_NAME, "tenant"),
            null,
            List.of(OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME))
        )
    );
  }

  private static ProjectionMetadata clusteredBaseProjection(List<TableClusterGroupSpec> groups)
  {
    return new ProjectionMetadata(
        groups.stream().mapToInt(TableClusterGroupSpec::getNumRows).sum(),
        new ClusteredValueGroupsBaseTableSchema(
            VirtualColumns.EMPTY,
            List.of(ColumnHolder.TIME_COLUMN_NAME, "tenant", "metric"),
            List.of(OrderBy.ascending("tenant"), OrderBy.ascending(ColumnHolder.TIME_COLUMN_NAME)),
            CLUSTERING_TENANT,
            null,
            new ClusteringDictionaries(List.of("acme", "globex", "initech"), null, null, null),
            groups
        )
    );
  }

  private static ProjectionMetadata projection(String name)
  {
    return new ProjectionMetadata(
        10,
        new AggregateProjectionSchema(
            name,
            null,
            null,
            VirtualColumns.EMPTY,
            List.of("tenant"),
            new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
            List.of(OrderBy.ascending("tenant"))
        )
    );
  }

  private static DataSegment anySegment()
  {
    return DataSegment.builder(SegmentId.of("ds", Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1)))
                      .size(0)
                      .build();
  }

  /**
   * Stub LoadSpec used to verify delegation. Uses the same JSON "type"=="stub" key as the test {@link #DELEGATE}.
   */
  @JsonTypeName("stub")
  public static class StubLoadSpec implements LoadSpec
  {
    static final AtomicInteger LOAD_CALLS = new AtomicInteger(0);
    static final AtomicInteger RANGE_CALLS = new AtomicInteger(0);

    private final String path;
    private final boolean supportsRange;

    @JsonCreator
    public StubLoadSpec(
        @JsonProperty("path") String path,
        @JsonProperty("supportsRange") @Nullable Boolean supportsRange
    )
    {
      this.path = path;
      this.supportsRange = supportsRange == null || supportsRange;
    }

    @JsonProperty
    public String getPath()
    {
      return path;
    }

    @JsonProperty
    public boolean isSupportsRange()
    {
      return supportsRange;
    }

    @Override
    public LoadSpecResult loadSegment(File destDir)
    {
      LOAD_CALLS.incrementAndGet();
      return new LoadSpecResult(42L);
    }

    @Override
    @Nullable
    public SegmentRangeReader openRangeReader()
    {
      if (!supportsRange) {
        return null;
      }
      RANGE_CALLS.incrementAndGet();
      return (filename, offset, length) -> new ByteArrayInputStream(new byte[0]);
    }
  }
}
