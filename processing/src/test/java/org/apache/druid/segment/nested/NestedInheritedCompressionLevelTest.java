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
import org.apache.druid.segment.serde.NestedCommonFormatColumnPartSerde;
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

  @Test
  public void testPersistedSerdePreservesCompressionLevelInheritance() throws Exception
  {
    final IndexSpec source = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(5)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(7)
        .withAutoColumnFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setObjectStorageCompression(CompressionStrategy.ZSTD)
                .setObjectStorageCompressionLevel(3)
                .build()
        )
        .build()
        .getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, source);

    final NestedCommonFormatColumnPartSerde.FormatSpec serde =
        NestedCommonFormatColumnPartSerde.FormatSpec.forSerde(effective);

    Assertions.assertNotNull(serde);
    Assertions.assertEquals(Integer.valueOf(3), serde.getObjectStorageCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(5), serde.getInheritedDictionaryCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(7), serde.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(7), serde.getInheritedDoubleCompressionLevel());

    final String json = TestHelper.JSON_MAPPER.writeValueAsString(serde);

    Assertions.assertTrue(json.contains("objectStorageCompressionLevel"));
    Assertions.assertFalse(json.contains("\"dictionaryEncodedColumnCompression\""));
    Assertions.assertFalse(json.contains("\"longColumnCompression\""));
    Assertions.assertFalse(json.contains("\"doubleColumnCompression\""));

    final NestedCommonFormatColumnPartSerde.FormatSpec restored =
        TestHelper.JSON_MAPPER.readValue(
            json,
            NestedCommonFormatColumnPartSerde.FormatSpec.class
        );

    Assertions.assertEquals(Integer.valueOf(3), restored.getObjectStorageCompressionLevel());

    final IndexSpec target = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(4)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(6)
        .build()
        .getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec merged =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(restored, target);

    Assertions.assertEquals(Integer.valueOf(4), merged.getInheritedDictionaryCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(6), merged.getInheritedLongCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(6), merged.getInheritedDoubleCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), merged.getObjectStorageCompressionLevel());
  }

  @Test
  public void testExplicitNestedCodecsRemainOverridesAfterSerde() throws Exception
  {
    final NestedCommonFormatColumnFormatSpec explicit =
        NestedCommonFormatColumnFormatSpec.builder()
            .setObjectStorageCompression(CompressionStrategy.ZSTD)
            .setObjectStorageCompressionLevel(3)
            .setDictionaryEncodedColumnCompression(CompressionStrategy.LZ4)
            .setLongColumnCompression(CompressionStrategy.ZSTD)
            .setDoubleColumnCompression(CompressionStrategy.ZSTD)
            .build();

    final String json = TestHelper.JSON_MAPPER.writeValueAsString(
        NestedCommonFormatColumnPartSerde.FormatSpec.forSerde(explicit)
    );

    Assertions.assertTrue(json.contains("\"dictionaryEncodedColumnCompression\""));
    Assertions.assertTrue(json.contains("\"longColumnCompression\""));
    Assertions.assertTrue(json.contains("\"doubleColumnCompression\""));

    final NestedCommonFormatColumnPartSerde.FormatSpec restored =
        TestHelper.JSON_MAPPER.readValue(
            json,
            NestedCommonFormatColumnPartSerde.FormatSpec.class
        );

    final NestedCommonFormatColumnFormatSpec merged =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(restored, indexSpec());

    Assertions.assertNull(merged.getInheritedDictionaryCompressionLevel());
    Assertions.assertNull(merged.getInheritedLongCompressionLevel());
    Assertions.assertNull(merged.getInheritedDoubleCompressionLevel());
    Assertions.assertEquals(Integer.valueOf(3), merged.getObjectStorageCompressionLevel());
  }

  @Test
  public void testDefaultCompressionLevelReinheritsAfterPersistence() throws Exception
  {
    final IndexSpec source = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .build()
        .getEffectiveSpec();

    Assertions.assertNull(source.getDimensionCompressionLevel());
    Assertions.assertNull(source.getMetricCompressionLevel());

    final NestedCommonFormatColumnFormatSpec sourceFormat =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, source);

    final String json = TestHelper.JSON_MAPPER.writeValueAsString(
        NestedCommonFormatColumnPartSerde.FormatSpec.forSerde(sourceFormat)
    );

    final NestedCommonFormatColumnPartSerde.FormatSpec restored =
        TestHelper.JSON_MAPPER.readValue(
            json,
            NestedCommonFormatColumnPartSerde.FormatSpec.class
        );

    final IndexSpec target = IndexSpec.builder()
        .withDimensionCompression(CompressionStrategy.LZ4)
        .withDimensionCompressionLevel(4)
        .withMetricCompression(CompressionStrategy.ZSTD)
        .withMetricCompressionLevel(6)
        .build()
        .getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec merged =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(restored, target);

    Assertions.assertEquals(
        Integer.valueOf(4),
        merged.getInheritedDictionaryCompressionLevel()
    );
    Assertions.assertEquals(
        Integer.valueOf(6),
        merged.getInheritedLongCompressionLevel()
    );
    Assertions.assertEquals(
        Integer.valueOf(6),
        merged.getInheritedDoubleCompressionLevel()
    );
  }

  @Test
  public void testEquivalentEffectiveFormatsIgnoreInheritanceProvenance()
  {
    final IndexSpec indexSpec = IndexSpec.getDefault().getEffectiveSpec();

    final NestedCommonFormatColumnFormatSpec inherited =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, indexSpec);

    final NestedCommonFormatColumnFormatSpec explicit =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setDictionaryEncodedColumnCompression(inherited.getDictionaryEncodedColumnCompression())
                .setLongColumnCompression(inherited.getLongColumnCompression())
                .setDoubleColumnCompression(inherited.getDoubleColumnCompression())
                .build(),
            indexSpec
        );

    Assertions.assertEquals(inherited, explicit);
    Assertions.assertEquals(inherited.hashCode(), explicit.hashCode());
  }
}
