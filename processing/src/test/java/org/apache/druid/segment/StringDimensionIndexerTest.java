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

import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Unit tests for {@link StringDimensionIndexer}.
 */
public class StringDimensionIndexerTest extends InitializedNullHandlingTest
{
  @Test
  public void testProcessRowValsToEncodedKeyComponent_usingAvgEstimates()
  {
    final StringDimensionIndexer indexer = new StringDimensionIndexer(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        true,
        false,
        false
    );

    long totalEstimatedSize = 0L;

    // Verify size for a non-empty dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "abc",
        new int[]{0},
        86L
    );

    // Verify size for null dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        null,
        new int[]{1},
        20L
    );

    // Verify size delta with repeated dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "abc",
        new int[]{0},
        20L
    );
    // Verify size delta with newly added dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "def",
        new int[]{2},
        86L
    );

    // Verify size delta for multi-values
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        Arrays.asList("abc", "def", "ghi"),
        new int[]{0, 2, 3},
        94L
    );

    Assert.assertEquals(306L, totalEstimatedSize);
  }

  @Test
  public void testProcessRowValsToEncodedKeyComponent_usingMaxEstimates()
  {
    final StringDimensionIndexer indexer = new StringDimensionIndexer(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        true,
        false,
        true
    );

    long totalEstimatedSize = 0L;

    // Verify size for a non-empty dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "abc",
        new int[]{0},
        54L
    );

    // Verify size for null dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        null,
        new int[]{1},
        4L
    );

    // Verify size delta with repeated dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "abc",
        new int[]{0},
        54L
    );
    // Verify size delta with newly added dimension value
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        "def",
        new int[]{2},
        54L
    );

    // Verify size delta for multi-values
    totalEstimatedSize += verifyEncodedValues(
        indexer,
        Arrays.asList("abc", "def", "ghi"),
        new int[]{0, 2, 3},
        162L
    );

    Assert.assertEquals(328L, totalEstimatedSize);
  }

  @Test
  public void testProcessRowValsToEncodedKeyComponent_comparison()
  {
    // Create indexers with useMaxMemoryEstimates = true/false
    final StringDimensionIndexer indexerForAvgEstimates = new StringDimensionIndexer(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        true,
        false,
        false
    );
    StringDimensionIndexer indexerForMaxEstimates = new StringDimensionIndexer(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        true,
        false,
        true
    );

    // Verify sizes with newly added dimension values
    long totalSizeWithMaxEstimates = 0L;
    long totalSizeWithAvgEstimates = 0L;
    for (int i = 0; i < 10; ++i) {
      final String dimValue = "value-" + i;
      totalSizeWithMaxEstimates += verifyEncodedValues(
          indexerForMaxEstimates,
          dimValue,
          new int[]{i},
          62L
      );
      totalSizeWithAvgEstimates += verifyEncodedValues(
          indexerForAvgEstimates,
          dimValue,
          new int[]{i},
          94L
      );
    }

    // If all dimension values are unique (or cardinality is high),
    // estimates with useMaxMemoryEstimates = false tend to be higher
    Assert.assertEquals(620L, totalSizeWithMaxEstimates);
    Assert.assertEquals(940L, totalSizeWithAvgEstimates);

    // Verify sizes with repeated dimension values
    for (int i = 0; i < 100; ++i) {
      final int index = i % 10;
      final String dimValue = "value-" + index;
      totalSizeWithMaxEstimates += verifyEncodedValues(
          indexerForMaxEstimates,
          dimValue,
          new int[]{index},
          62L
      );
      totalSizeWithAvgEstimates += verifyEncodedValues(
          indexerForAvgEstimates,
          dimValue,
          new int[]{index},
          20L
      );
    }

    // If dimension values are frequently repeated (cardinality is low),
    // estimates with useMaxMemoryEstimates = false tend to be much lower
    Assert.assertEquals(6820L, totalSizeWithMaxEstimates);
    Assert.assertEquals(2940L, totalSizeWithAvgEstimates);
  }

  private long verifyEncodedValues(
      StringDimensionIndexer indexer,
      Object dimensionValues,
      int[] expectedEncodedValues,
      long expectedSizeDelta
  )
  {
    EncodedKeyComponent<int[]> encodedKeyComponent = indexer
        .processRowValsToUnsortedEncodedKeyComponent(dimensionValues, false);
    Assert.assertArrayEquals(expectedEncodedValues, encodedKeyComponent.getComponent());
    Assert.assertEquals(expectedSizeDelta, encodedKeyComponent.getEffectiveSizeBytes());

    return encodedKeyComponent.getEffectiveSizeBytes();
  }

}
