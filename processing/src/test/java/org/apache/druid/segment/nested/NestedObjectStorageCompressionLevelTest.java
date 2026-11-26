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

public class NestedObjectStorageCompressionLevelTest
{
  @Test
  public void testRoundTripJson() throws Exception
  {
    final NestedCommonFormatColumnFormatSpec original = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.ZSTD)
        .setObjectStorageCompressionLevel(3)
        .build();
    final String json = TestHelper.JSON_MAPPER.writeValueAsString(original);
    Assertions.assertTrue(json.contains("objectStorageCompressionLevel"));
    Assertions.assertEquals(
        original,
        TestHelper.JSON_MAPPER.readValue(json, NestedCommonFormatColumnFormatSpec.class)
    );
  }

  @Test
  public void testUnsetLevelDoesNotChangeJson() throws Exception
  {
    final NestedCommonFormatColumnFormatSpec original = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.LZ4)
        .build();
    final String json = TestHelper.JSON_MAPPER.writeValueAsString(original);
    Assertions.assertFalse(json.contains("objectStorageCompressionLevel"));
  }

  @Test
  public void testLevelRequiresExplicitCompression()
  {
    Assertions.assertThrows(
        IllegalArgumentException.class,
        () -> NestedCommonFormatColumnFormatSpec.builder().setObjectStorageCompressionLevel(3).build()
    );
  }

  @Test
  public void testInvalidCodecLevels()
  {
    for (int level : new int[]{-1, 18}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> NestedCommonFormatColumnFormatSpec.builder()
              .setObjectStorageCompression(CompressionStrategy.LZ4)
              .setObjectStorageCompressionLevel(level)
              .build()
      );
    }
    for (int level : new int[]{-1, 23}) {
      Assertions.assertThrows(
          IllegalArgumentException.class,
          () -> NestedCommonFormatColumnFormatSpec.builder()
              .setObjectStorageCompression(CompressionStrategy.ZSTD)
              .setObjectStorageCompressionLevel(level)
              .build()
      );
    }
  }

  @Test
  public void testIgnoredForUnlevelledCodec()
  {
    final NestedCommonFormatColumnFormatSpec partial = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.LZF)
        .setObjectStorageCompressionLevel(5)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(partial, IndexSpec.getDefault().getEffectiveSpec());
    Assertions.assertNull(effective.getObjectStorageCompressionLevel());
    Assertions.assertEquals(CompressionStrategy.LZF, effective.getObjectStorageCompression());
  }

  @Test
  public void testDefaultLevelInherited()
  {
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(null, defaultSpec());
    Assertions.assertEquals(CompressionStrategy.ZSTD, effective.getObjectStorageCompression());
    Assertions.assertEquals(Integer.valueOf(3), effective.getObjectStorageCompressionLevel());
  }

  @Test
  public void testOverrideCodecDoesNotInheritDifferentCodecLevel()
  {
    final NestedCommonFormatColumnFormatSpec column = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.LZ4)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(column, defaultSpec());
    Assertions.assertEquals(CompressionStrategy.LZ4, effective.getObjectStorageCompression());
    Assertions.assertNull(effective.getObjectStorageCompressionLevel());
  }

  @Test
  public void testColumnLevelOverridesDefault()
  {
    final NestedCommonFormatColumnFormatSpec column = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.ZSTD)
        .setObjectStorageCompressionLevel(7)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(column, defaultSpec());
    Assertions.assertEquals(Integer.valueOf(7), effective.getObjectStorageCompressionLevel());
  }

  @Test
  public void testSameCodecInheritsDefaultLevel()
  {
    final NestedCommonFormatColumnFormatSpec column = NestedCommonFormatColumnFormatSpec.builder()
        .setObjectStorageCompression(CompressionStrategy.ZSTD)
        .build();
    final NestedCommonFormatColumnFormatSpec effective =
        NestedCommonFormatColumnFormatSpec.getEffectiveFormatSpec(column, defaultSpec());
    Assertions.assertEquals(Integer.valueOf(3), effective.getObjectStorageCompressionLevel());
  }

  private static IndexSpec defaultSpec()
  {
    return IndexSpec.builder()
        .withAutoColumnFormatSpec(
            NestedCommonFormatColumnFormatSpec.builder()
                .setObjectStorageCompression(CompressionStrategy.ZSTD)
                .setObjectStorageCompressionLevel(3)
                .build()
        )
        .build()
        .getEffectiveSpec();
  }
}
