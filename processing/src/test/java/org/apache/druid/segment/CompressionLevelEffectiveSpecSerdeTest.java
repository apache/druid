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

package org.apache.druid.segment;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Diagnostic regression test: effective IndexSpec settings must preserve nested
 * column compression inheritance when serialized and reconstructed, as when
 * effective specifications pass through segment metadata and compaction state.
 */
public class CompressionLevelEffectiveSpecSerdeTest
{
  @Test
  public void testNestedCompressionLevelSurvivesEffectiveSpecJsonRoundTrip() throws Exception
  {
    final IndexSpec effective = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(0)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(3)
        .withAutoColumnFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setObjectStorageCompression(CompressionStrategy.ZSTD)
                .setObjectStorageCompressionLevel(3)
                .build()
        )
        .build()
        .getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec before =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, effective);
    Assertions.assertEquals(Integer.valueOf(0), before.getInheritedDictionaryCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), before.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), before.getInheritedDoubleCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), before.getObjectStorageCompressionLevel());

    final ObjectMapper mapper = new DefaultObjectMapper();
    final IndexSpec restored = mapper.readValue(mapper.writeValueAsBytes(effective), IndexSpec.class)
                                     .getEffectiveSpec();
    final NestedCommonFormatColumnFormatSpec after =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, restored);

    Assertions.assertEquals(before.getInheritedDictionaryCompressionLevel(),
                            after.getInheritedDictionaryCompressionLevel(),
                            "Nested dictionary compression level changed after IndexSpec JSON round trip");
    Assertions.assertEquals(before.getInheritedLongCompressionLevel(),
                            after.getInheritedLongCompressionLevel(),
                            "Nested LONG compression level changed after IndexSpec JSON round trip");
    Assertions.assertEquals(before.getInheritedDoubleCompressionLevel(),
                            after.getInheritedDoubleCompressionLevel(),
                            "Nested DOUBLE compression level changed after IndexSpec JSON round trip");
    Assertions.assertEquals(before.getObjectStorageCompressionLevel(),
                            after.getObjectStorageCompressionLevel(),
                            "Nested raw-value compression level changed after IndexSpec JSON round trip");
  }

  @Test
  public void testExplicitNestedCodecRemainsExplicitAfterJsonRoundTrip() throws Exception
  {
    final IndexSpec effective = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(0)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(3)
        .withAutoColumnFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setDictionaryEncodedColumnCompression(CompressionStrategy.LZ4)
                .setLongColumnCompression(CompressionStrategy.ZSTD)
                .setDoubleColumnCompression(CompressionStrategy.ZSTD)
                .build()
        )
        .build()
        .getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec before =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, effective);
    Assertions.assertNull(before.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(before.getInheritedLongCompressionLevel());
    Assertions.assertNull(before.getInheritedDoubleCompressionLevel());

    final ObjectMapper mapper = new DefaultObjectMapper();
    final IndexSpec restored = mapper.readValue(mapper.writeValueAsBytes(effective), IndexSpec.class)
                                     .getEffectiveSpec();
    final NestedCommonFormatColumnFormatSpec after =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, restored);

    Assertions.assertEquals(CompressionStrategy.LZ4, after.getDictionaryEncodedColumnCompression());
    Assertions.assertEquals(CompressionStrategy.ZSTD, after.getLongColumnCompression());
    Assertions.assertEquals(CompressionStrategy.ZSTD, after.getDoubleColumnCompression());
    Assertions.assertNull(after.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(after.getInheritedLongCompressionLevel());
    Assertions.assertNull(after.getInheritedDoubleCompressionLevel());
  }

  @Test
  public void testExplicitNestedCodecJsonStillRoundTrips() throws Exception
  {
    final NestedCommonFormatColumnFormatSpec original = NestedCommonFormatColumnFormatSpec.builder()
        .setDictionaryEncodedColumnCompression(CompressionStrategy.ZSTD)
        .setLongColumnCompression(CompressionStrategy.LZ4)
        .setDoubleColumnCompression(CompressionStrategy.ZSTD)
        .build();
    final ObjectMapper mapper = new DefaultObjectMapper();
    final String json = mapper.writeValueAsString(original);
    Assertions.assertTrue(json.contains("\"dictionaryEncodedColumnCompression\""));
    Assertions.assertTrue(json.contains("\"longColumnCompression\""));
    Assertions.assertTrue(json.contains("\"doubleColumnCompression\""));
    final NestedCommonFormatColumnFormatSpec restored =
        mapper.readValue(json, NestedCommonFormatColumnFormatSpec.class);
    Assertions.assertEquals(original.getDictionaryEncodedColumnCompression(),
                            restored.getDictionaryEncodedColumnCompression());
    Assertions.assertEquals(original.getLongColumnCompression(), restored.getLongColumnCompression());
    Assertions.assertEquals(original.getDoubleColumnCompression(), restored.getDoubleColumnCompression());
  }

}
