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
import org.apache.druid.segment.file.SegmentFileBuilder;
import org.apache.druid.segment.file.SegmentFileContainerMetadata;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.Projections;
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

class PartialFullSegmentLoadSpecTest
{
  private static final Map<String, Object> DELEGATE = ImmutableMap.of(
      "type", "stub",
      "path", "/var/druid/segments/foo"
  );

  private static ObjectMapper configuredMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.registerSubtypes(PartialFullSegmentLoadSpec.class, StubLoadSpec.class);
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    PartialFullSegmentLoadSpec spec = spec();
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(PartialFullSegmentLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialFullSegmentType() throws Exception
  {
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec()),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialFullSegment", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(PartialFullSegmentLoadSpec.FINGERPRINT, wireForm.get("fingerprint"));
    // No scheme-specific field: the selection is the segment's whole layout.
    Assertions.assertEquals(3, wireForm.size());
  }

  @Test
  void testFingerprintIsDistinctFromOtherLayoutDerivedLoads()
  {
    // A full-segment load and a base-table load put different amounts on the historical, and an empty load puts
    // nothing there. Sharing a fingerprint would hide a rule swap from the coordinator's reconciliation.
    Assertions.assertNotEquals(PartialBaseTableLoadSpec.FINGERPRINT, PartialFullSegmentLoadSpec.FINGERPRINT);
    Assertions.assertNotEquals("v1:partial-empty", PartialFullSegmentLoadSpec.FINGERPRINT);
  }

  @Test
  void testGetSelectedBundleNamesSelectsEveryBundle()
  {
    // Base table, a cluster group and a projection: all of them, no layout branching.
    final String groupBundle = Projections.getClusterGroupBundleName(List.of(0));
    final List<String> bundles = List.of(Projections.BASE_TABLE_PROJECTION_NAME, groupBundle, "user_hourly");
    Assertions.assertEquals(bundles, spec().getSelectedBundleNames(anySegment(), metadata(bundles)));
  }

  @Test
  void testGetSelectedBundleNamesDedupesContainersSharingABundle()
  {
    // A bundle spans as many containers as the writer needed; the selection is by name.
    final SegmentFileMetadata metadata = metadata(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME, Projections.BASE_TABLE_PROJECTION_NAME, "user_hourly")
    );
    Assertions.assertEquals(
        List.of(Projections.BASE_TABLE_PROJECTION_NAME, "user_hourly"),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesLegacyRootOnlySegment()
  {
    final SegmentFileMetadata metadata = metadata(List.of(SegmentFileBuilder.ROOT_BUNDLE_NAME));
    Assertions.assertEquals(
        List.of(SegmentFileBuilder.ROOT_BUNDLE_NAME),
        spec().getSelectedBundleNames(anySegment(), metadata)
    );
  }

  @Test
  void testGetSelectedBundleNamesThrowsWhenNoContainers()
  {
    final SegmentFileMetadata metadata = metadata(List.of());
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec().getSelectedBundleNames(anySegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("metadata declares no containers"),
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
    PartialFullSegmentLoadSpec spec = new PartialFullSegmentLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        PartialFullSegmentLoadSpec.FINGERPRINT,
        jsonMapper
    );
    Assertions.assertNull(spec.openRangeReader());
  }

  @Test
  void testRejectsNullDelegate()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialFullSegmentLoadSpec(null, PartialFullSegmentLoadSpec.FINGERPRINT, jsonMapper)
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialFullSegmentLoadSpec(DELEGATE, null, jsonMapper)
    );
  }

  private PartialFullSegmentLoadSpec spec()
  {
    return new PartialFullSegmentLoadSpec(DELEGATE, PartialFullSegmentLoadSpec.FINGERPRINT, jsonMapper);
  }

  /**
   * Metadata carrying one container per entry of {@code bundleNames} — this spec only reads which bundles exist, not
   * what is in them, and no projection list is needed since it does no layout branching.
   */
  private static SegmentFileMetadata metadata(List<String> bundleNames)
  {
    final List<SegmentFileContainerMetadata> containers = new ArrayList<>(bundleNames.size());
    long offset = 0;
    for (String bundleName : bundleNames) {
      containers.add(new SegmentFileContainerMetadata(offset, 100L, bundleName));
      offset += 100L;
    }
    return new SegmentFileMetadata(containers, Map.of(), null, null, null, null, null);
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
