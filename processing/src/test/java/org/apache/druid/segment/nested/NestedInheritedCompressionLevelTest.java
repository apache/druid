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

package org.apache.druid.segment.nested;

import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class NestedInheritedCompressionLevelTest
{
  private static IndexSpec indexSpec()
  {
    return IndexSpec.builder()
                    .withDimensionCompression(CompressionStrategy.LZ4)
                    .withDimensionCompressionLevel(0)
                    .withMetricCompression(CompressionStrategy.ZSTD)
                    .withMetricCompressionLevel(3)
                    .build()
                    .getEffectiveSpec();
  }

  @Test
  public void testLevelsInheritFromIndexSpec()
  {
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec());
    Assertions.assertEquals(Integer.valueOf(0), effective.getInheritedDictionaryCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), effective.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), effective.getInheritedDoubleCompressionLevel());
  }

  @Test
  public void testExplicitCodecDoesNotInheritEvenIfItMatches()
  {
    final NestedCommonFormatColumnFormatSpec column = NestedCommonFormatColumnFormatSpec.builder()
        .setDictionaryEncodedColumnCompression(CompressionStrategy.LZ4)
        .setLongColumnCompression(CompressionStrategy.ZSTD)
        .setDoubleColumnCompression(CompressionStrategy.ZSTD)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(column, indexSpec());
    Assertions.assertNull(effective.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(effective.getInheritedLongCompressionLevel());
    Assertions.assertNull(effective.getInheritedDoubleCompressionLevel());
  }

  @Test
  public void testExplicitDifferentCodecDoesNotInherit()
  {
    final NestedCommonFormatColumnFormatSpec column = NestedCommonFormatColumnFormatSpec.builder()
        .setDictionaryEncodedColumnCompression(CompressionStrategy.ZSTD)
        .setLongColumnCompression(CompressionStrategy.LZ4)
        .setDoubleColumnCompression(CompressionStrategy.LZ4)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(column, indexSpec());
    Assertions.assertNull(effective.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(effective.getInheritedLongCompressionLevel());
    Assertions.assertNull(effective.getInheritedDoubleCompressionLevel());
  }

  @Test
  public void testUncompressedDictionaryGetsNoLevel()
  {
    final IndexSpec indexSpec = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.UNCOMPRESSED)
        .withDimensionCompressionLevel(8)
        .build()
        .getEffectiveSpec();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec);
    Assertions.assertEquals(CompressionStrategy.UNCOMPRESSED, effective.getDictionaryEncodedColumnCompression());
    Assertions.assertNull(effective.getInheritedDictionaryCompressionLevel());
  }

  @Test
  public void testInheritedLevelsNotExposedInNestedJson() throws Exception
  {
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec());
    final String json = TestHelper.JSON_MAPPER.writeValueAsString(effective);
    Assertions.assertFalse(json.contains("inheritedDictionaryCompressionLevel"));
    Assertions.assertFalse(json.contains("inheritedLongCompressionLevel"));
    Assertions.assertFalse(json.contains("inheritedDoubleCompressionLevel"));
  }

  @Test
  public void testAutoColumnDefaultPreservesInheritedLevels()
  {
    final IndexSpec indexSpec = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(0)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(3)
        .withAutoColumnFormatSpec(NestedCommonFormatColumnFormatSpec.builder().build())
        .build()
        .getEffectiveSpec();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec);
    Assertions.assertEquals(Integer.valueOf(0), effective.getInheritedDictionaryCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), effective.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), effective.getInheritedDoubleCompressionLevel());
  }

  @Test
  public void testAutoColumnExplicitCompressionDoesNotInheritLevel()
  {
    final IndexSpec indexSpec = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(0)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(3)
        .withAutoColumnFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setDictionaryEncodedColumnCompression(CompressionStrategy.LZ4)
                .setLongColumnCompression(CompressionStrategy.ZSTD)
                .build()
        )
        .build()
        .getEffectiveSpec();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec);
    Assertions.assertNull(effective.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(effective.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), effective.getInheritedDoubleCompressionLevel());
  }
}
