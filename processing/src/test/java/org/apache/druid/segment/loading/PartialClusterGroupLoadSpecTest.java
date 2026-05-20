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
import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import javax.annotation.Nullable;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

class PartialClusterGroupLoadSpecTest
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
    module.registerSubtypes(PartialClusterGroupLoadSpec.class, StubLoadSpec.class);
    m.registerModule(module);
    m.setInjectableValues(new InjectableValues.Std().addValue(ObjectMapper.class, m));
    return m;
  }

  private final ObjectMapper jsonMapper = configuredMapper();

  @Test
  void testJsonRoundTrip() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0, 2, 5),
        FINGERPRINT,
        jsonMapper
    );
    String json = jsonMapper.writeValueAsString(spec);
    LoadSpec reread = jsonMapper.readValue(json, LoadSpec.class);
    Assertions.assertInstanceOf(PartialClusterGroupLoadSpec.class, reread);
    Assertions.assertEquals(spec, reread);
  }

  @Test
  void testWireFormHasPartialClusterGroupType() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0, 1),
        FINGERPRINT,
        jsonMapper
    );
    Map<String, Object> wireForm = jsonMapper.readValue(
        jsonMapper.writeValueAsString(spec),
        new TypeReference<>()
        {
        }
    );
    Assertions.assertEquals("partialClusterGroup", wireForm.get("type"));
    Assertions.assertEquals(DELEGATE, wireForm.get("delegate"));
    Assertions.assertEquals(List.of(0, 1), wireForm.get("clusterGroupIndices"));
    Assertions.assertEquals(FINGERPRINT, wireForm.get("fingerprint"));
  }

  @Test
  void testLoadSegmentDelegatesToInner() throws Exception
  {
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
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
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        DELEGATE,
        List.of(0),
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
    PartialClusterGroupLoadSpec spec = new PartialClusterGroupLoadSpec(
        ImmutableMap.of("type", "stub", "path", "/", "supportsRange", false),
        List.of(0),
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
        () -> new PartialClusterGroupLoadSpec(null, List.of(0), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullOrEmptyClusterGroupIndices()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PartialClusterGroupLoadSpec(DELEGATE, null, "v1:x", jsonMapper)
    );
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> new PartialClusterGroupLoadSpec(DELEGATE, List.of(), "v1:x", jsonMapper)
    );
  }

  @Test
  void testRejectsNullFingerprint()
  {
    Assertions.assertThrows(
        NullPointerException.class,
        () -> new PartialClusterGroupLoadSpec(DELEGATE, List.of(0), null, jsonMapper)
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
