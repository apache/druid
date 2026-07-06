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
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.projections.AggregateProjectionSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class PartialProjectionLoadSpecTest
{
  private static final Map<String, Object> DELEGATE = ImmutableMap.of(
      "type", "stub",
      "path", "/var/druid/segments/foo"
  );
  private static final String FINGERPRINT = "v1:abcdef0123456789";

  private static ObjectMapper configuredMapper()
  {
    final ObjectMapper m = new DefaultObjectMapper();
    final SimpleModule module = new SimpleModule();
    module.registerSubtypes(PartialProjectionLoadSpec.class, StubLoadSpec.class);
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("user_daily", "user_hourly"),
        FINGERPRINT,
        jsonMapper
    );
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(PartialProjectionLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialProjectionType() throws Exception
  {
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("a"),
        FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialProjection", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(List.of("a"), wireForm.get("projections"));
    Assertions.assertEquals(FINGERPRINT, wireForm.get("fingerprint"));
  }

  @Test
  void testLoadSegmentDelegatesToInner() throws Exception
  {
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("a"),
        FINGERPRINT,
        jsonMapper
    );
    StubLoadSpec.LOAD_CALLS.set(0);
    LoadSpec.LoadSpecResult result = spec.loadSegment(new File("/tmp/dest"));
    Assertions.assertEquals(1, StubLoadSpec.LOAD_CALLS.get());
    Assertions.assertEquals(42L, result.getSize());
  }

  @Test
  void testOpenRangeReaderDelegatesToInner() throws Exception
  {
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("a"),
        FINGERPRINT,
        jsonMapper
    );
    StubLoadSpec.RANGE_CALLS.set(0);
    SegmentRangeReader reader = spec.openRangeReader();
    Assertions.assertNotNull(reader);
    Assertions.assertEquals(1, StubLoadSpec.RANGE_CALLS.get());
  }

  @Test
  void testOpenRangeReaderReturnsNullWhenInnerDoesNotSupport() throws Exception
  {
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        List.of("a"),
        FINGERPRINT,
        jsonMapper
    );
    Assertions.assertNull(spec.openRangeReader());
  }

  @Test
  void testRejectsNullDelegate()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialProjectionLoadSpec(null, List.of("a"), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullOrEmptyProjections()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PartialProjectionLoadSpec(DELEGATE, null, "v1:x", jsonMapper)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PartialProjectionLoadSpec(DELEGATE, List.of(), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialProjectionLoadSpec(DELEGATE, List.of("a"), null, jsonMapper)
    );
  }

  @Test
  void testGetSelectedBundleNamesReturnsProjections()
  {
    // Projection names double as bundle names; each requested name must appear in the segment's projections.
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("user_daily", "user_hourly"),
        FINGERPRINT,
        jsonMapper
    );
    final SegmentFileMetadata metadata = metadataWithProjections("user_daily", "user_hourly", "some_other");
    Assertions.assertEquals(List.of("user_daily", "user_hourly"), spec.getSelectedBundleNames(anySegment(), metadata));
  }

  @Test
  void testGetSelectedBundleNamesThrowsWhenProjectionMissing()
  {
    // Defensive: a wire form referring to a projection this segment doesn't have. Should only happen on
    // matcher/reader drift or a coding bug
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("user_daily", "vanished"),
        FINGERPRINT,
        jsonMapper
    );
    final SegmentFileMetadata metadata = metadataWithProjections("user_daily");
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(anySegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("does not contain projection[vanished]"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testGetSelectedBundleNamesThrowsWhenMetadataHasNoProjections()
  {
    // Defensive: a segment with no projections in metadata can't satisfy any projection rule.
    PartialProjectionLoadSpec spec = new PartialProjectionLoadSpec(
        DELEGATE,
        List.of("user_daily"),
        FINGERPRINT,
        jsonMapper
    );
    final SegmentFileMetadata metadata = new SegmentFileMetadata(List.of(), Map.of(), null, null, null, null);
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> spec.getSelectedBundleNames(anySegment(), metadata)
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("metadata has no projections"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  private static DataSegment anySegment()
  {
    return DataSegment.builder(SegmentId.of("ds", Intervals.ETERNITY, "v1", new NumberedShardSpec(0, 1)))
                      .size(0)
                      .build();
  }

  private static SegmentFileMetadata metadataWithProjections(String... names)
  {
    final List<ProjectionMetadata> projections = new java.util.ArrayList<>(names.length);
    for (String name : names) {
      projections.add(new ProjectionMetadata(1, projectionSchemaNamed(name)));
    }
    return new SegmentFileMetadata(List.of(), Map.of(), null, null, projections, null);
  }

  private static AggregateProjectionSchema projectionSchemaNamed(String name)
  {
    return new AggregateProjectionSchema(
        name,
        null,
        null,
        VirtualColumns.EMPTY,
        List.of("dim1"),
        new AggregatorFactory[]{new CountAggregatorFactory("cnt")},
        List.of(OrderBy.ascending("dim1"))
    );
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
