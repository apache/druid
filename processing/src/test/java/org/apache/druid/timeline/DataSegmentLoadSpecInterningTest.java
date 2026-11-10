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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import it.unimi.dsi.fastutil.objects.Object2ObjectArrayMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests how {@link DataSegment} prepares its load spec: compacting nested maps and interning the values that repeat
 * across segments, while leaving the per-segment values and the serialized form alone. Segments are read from JSON,
 * like they are in practice, so each starts with its own copy of every value.
 */
public class DataSegmentLoadSpecInterningTest
{
  private static final ObjectMapper MAPPER = new DefaultObjectMapper();

  static {
    MAPPER.setInjectableValues(
        new InjectableValues.Std().addValue(DataSegment.PruneSpecsHolder.class, DataSegment.PruneSpecsHolder.DEFAULT)
    );
  }

  private static final String S3_LOAD_SPEC =
      "{\"type\":\"s3_zip\",\"bucket\":\"my-bucket\",\"key\":\"segments/index.zip\",\"S3Schema\":\"s3n\"}";

  private static final String PARTIAL_LOAD_SPEC =
      "{\"type\":\"partialProjection\","
      + "\"delegate\":" + S3_LOAD_SPEC + ","
      + "\"projections\":[\"daily\",\"hourly\"],"
      + "\"fingerprint\":\"v1:0123456789abcdef\"}";

  private static final String COMPOSITE_LOAD_SPEC =
      "{\"type\":\"partialComposite\","
      + "\"delegate\":" + S3_LOAD_SPEC + ","
      + "\"members\":[{\"type\":\"partialProjection\",\"projections\":[\"daily\"],\"fingerprint\":\"v1:aaaa\"}],"
      + "\"fingerprint\":\"v1:bbbb\"}";

  @Test
  public void testRepeatedValuesAreInterned() throws Exception
  {
    final Map<String, Object> a = readSegment(S3_LOAD_SPEC, 0).getLoadSpec();
    final Map<String, Object> b = readSegment(S3_LOAD_SPEC, 1).getLoadSpec();

    Assertions.assertSame(a.get("type"), b.get("type"));
    Assertions.assertSame(a.get("bucket"), b.get("bucket"));
    Assertions.assertSame(a.get("S3Schema"), b.get("S3Schema"));
    // per-segment in practice, so interning it would only add an interner entry; equal here only to show it isn't
    Assertions.assertEquals(a.get("key"), b.get("key"));
    Assertions.assertNotSame(a.get("key"), b.get("key"));
  }

  @Test
  public void testPartialLoadSpecIsCompactedAndInterned() throws Exception
  {
    final Map<String, Object> a = readSegment(PARTIAL_LOAD_SPEC, 0).getLoadSpec();
    final Map<String, Object> b = readSegment(PARTIAL_LOAD_SPEC, 1).getLoadSpec();

    Assertions.assertSame(a.get("type"), b.get("type"));
    Assertions.assertSame(a.get("fingerprint"), b.get("fingerprint"));
    final List<?> projectionsA = (List<?>) a.get("projections");
    final List<?> projectionsB = (List<?>) b.get("projections");
    for (int i = 0; i < projectionsA.size(); i++) {
      Assertions.assertSame(projectionsA.get(i), projectionsB.get(i));
    }

    // the delegate is compacted like the top level, and its repeated values are interned
    final Map<?, ?> delegateA = (Map<?, ?>) a.get("delegate");
    final Map<?, ?> delegateB = (Map<?, ?>) b.get("delegate");
    Assertions.assertInstanceOf(Object2ObjectArrayMap.class, delegateA);
    Assertions.assertSame(delegateA.get("type"), delegateB.get("type"));
    Assertions.assertSame(delegateA.get("bucket"), delegateB.get("bucket"));
    Assertions.assertNotSame(delegateA.get("key"), delegateB.get("key"));
  }

  @Test
  public void testCompositeMembersAreCompactedAndInterned() throws Exception
  {
    final Map<String, Object> a = readSegment(COMPOSITE_LOAD_SPEC, 0).getLoadSpec();
    final Map<String, Object> b = readSegment(COMPOSITE_LOAD_SPEC, 1).getLoadSpec();

    final Map<?, ?> memberA = (Map<?, ?>) ((List<?>) a.get("members")).get(0);
    final Map<?, ?> memberB = (Map<?, ?>) ((List<?>) b.get("members")).get(0);
    Assertions.assertInstanceOf(Object2ObjectArrayMap.class, memberA);
    Assertions.assertSame(memberA.get("fingerprint"), memberB.get("fingerprint"));
    Assertions.assertSame(
        ((List<?>) memberA.get("projections")).get(0),
        ((List<?>) memberB.get("projections")).get(0)
    );
  }

  @Test
  public void testSerializedFormIsUnchanged() throws Exception
  {
    for (String loadSpecJson : List.of(S3_LOAD_SPEC, PARTIAL_LOAD_SPEC, COMPOSITE_LOAD_SPEC)) {
      final DataSegment segment = readSegment(loadSpecJson, 0);
      // equal to the map it was read from, and written back in the same order
      Assertions.assertEquals(
          MAPPER.readValue(loadSpecJson, JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT),
          segment.getLoadSpec()
      );
      Assertions.assertEquals(loadSpecJson, MAPPER.writeValueAsString(segment.getLoadSpec()));
    }
  }

  @Test
  public void testNullValuesArePreserved()
  {
    final Map<String, Object> nested = new HashMap<>();
    nested.put("type", null);
    nested.put("path", "/tmp/segment");
    final Map<String, Object> loadSpec = new HashMap<>();
    loadSpec.put("type", "local");
    loadSpec.put("path", null);
    loadSpec.put("nested", nested);

    final DataSegment segment =
        DataSegment.builder(SegmentId.of("ds", Intervals.of("2026-01-01/2026-01-02"), "v1", 0))
                   .loadSpec(loadSpec)
                   .size(1)
                   .build();

    Assertions.assertEquals(loadSpec, segment.getLoadSpec());
  }

  private static DataSegment readSegment(String loadSpecJson, int partitionNum) throws Exception
  {
    return MAPPER.readValue(
        StringUtils.format(
            "{\"dataSource\":\"ds\",\"interval\":\"2026-01-01/2026-01-02\",\"version\":\"v1\",\"loadSpec\":%s,"
            + "\"shardSpec\":{\"type\":\"numbered\",\"partitionNum\":%d,\"partitions\":0},"
            + "\"binaryVersion\":9,\"size\":1}",
            loadSpecJson,
            partitionNum
        ),
        DataSegment.class
    );
  }
}
