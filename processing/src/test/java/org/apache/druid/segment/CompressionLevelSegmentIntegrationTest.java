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

import com.google.common.collect.Lists;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.guice.BuiltInTypesModule;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.DoubleSumAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.nested.NestedCommonFormatColumnFormatSpec;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.testing.TemporaryFolderExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Full persist/merge/load regression for non-default compression levels.
 * The baseline uses the same codecs without explicit levels; the tuned segment
 * must round-trip the same scalar, nested, variant, and metric values.
 */
public class CompressionLevelSegmentIntegrationTest extends InitializedNullHandlingTest
{
  @RegisterExtension
  public final TemporaryFolderExtension temporaryFolder = TemporaryFolderExtension.testCaseScoped();

  @BeforeAll
  public static void registerNestedSerdes()
  {
    BuiltInTypesModule.registerHandlersAndSerde();
  }

  @Test
  public void testLz4V9RoundTrip() throws IOException
  {
    verifyRoundTrip(false, CompressionStrategy.LZ4, 5, 8, 11);
  }

  @Test
  public void testZstdV9RoundTrip() throws IOException
  {
    verifyRoundTrip(false, CompressionStrategy.ZSTD, 3, 5, 6);
  }

  @Test
  public void testLz4V10RoundTrip() throws IOException
  {
    verifyRoundTrip(true, CompressionStrategy.LZ4, 5, 8, 11);
  }

  @Test
  public void testZstdV10RoundTrip() throws IOException
  {
    verifyRoundTrip(true, CompressionStrategy.ZSTD, 3, 5, 6);
  }

  private void verifyRoundTrip(
      boolean useV10,
      CompressionStrategy codec,
      int dimensionLevel,
      int metricLevel,
      int objectStorageLevel
  ) throws IOException
  {
    final IndexSpec baselineSpec = IndexSpec.builder()
                                            .withDimensionCompression(codec)
                                            .withMetricCompression(codec)
                                            .withComplexMetricCompression(codec)
                                            .withAutoColumnFormatSpec(
                                                NestedCommonFormatColumnFormatSpec.builder()
                                                                                .setObjectStorageCompression(codec)
                                                                                .build()
                                            )
                                            .build();

    final IndexSpec tunedSpec = IndexSpec.builder()
                                         .withDimensionCompression(codec)
                                         .withDimensionCompressionLevel(dimensionLevel)
                                         .withMetricCompression(codec)
                                         .withMetricCompressionLevel(metricLevel)
                                         .withComplexMetricCompression(codec)
                                         .withComplexMetricCompressionLevel(metricLevel)
                                         .withAutoColumnFormatSpec(
                                             NestedCommonFormatColumnFormatSpec.builder()
                                                                             .setObjectStorageCompression(codec)
                                                                             .setObjectStorageCompressionLevel(
                                                                                 objectStorageLevel
                                                                             )
                                                                             .build()
                                         )
                                         .build();

    try (QueryableIndex baseline = buildIndex(useV10, baselineSpec);
         QueryableIndex tuned = buildIndex(useV10, tunedSpec)) {
      Assertions.assertEquals(4, baseline.getNumRows());
      Assertions.assertEquals(baseline.getNumRows(), tuned.getNumRows());
      Assertions.assertEquals(baseline.getColumnNames(), tuned.getColumnNames());
      Assertions.assertEquals(
          Lists.newArrayList(baseline.getAvailableDimensions()),
          Lists.newArrayList(tuned.getAvailableDimensions())
      );

      final List<DebugRow> expected = RowIteratorHelper.toList(new QueryableIndexIndexableAdapter(baseline).getRows());
      final List<DebugRow> actual = RowIteratorHelper.toList(new QueryableIndexIndexableAdapter(tuned).getRows());
      Assertions.assertEquals(expected.size(), actual.size());
      for (int i = 0; i < expected.size(); i++) {
        Assertions.assertEquals(expected.get(i).dimensionValues(), actual.get(i).dimensionValues());
        Assertions.assertEquals(expected.get(i).metricValues(), actual.get(i).metricValues());
      }
    }
  }

  private QueryableIndex buildIndex(boolean useV10, IndexSpec spec) throws IOException
  {
    final IncrementalIndexSchema schema = new IncrementalIndexSchema.Builder()
        .withDimensionsSpec(
            DimensionsSpec.builder()
                          .setDimensions(
                              List.of(
                                  new StringDimensionSchema("dim"),
                                  new LongDimensionSchema("longDim"),
                                  AutoTypeColumnSchema.of("nested"),
                                  AutoTypeColumnSchema.of("variant")
                              )
                          )
                          .build()
        )
        .withMetrics(
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("longSum", "longVal"),
            new DoubleSumAggregatorFactory("doubleSum", "doubleVal")
        )
        .withRollup(false)
        .withMinTimestamp(0L)
        .build();

    final IndexBuilder builder = IndexBuilder.create()
                                             .schema(schema)
                                             .indexSpec(spec)
                                             .tmpDir(temporaryFolder.newFolder())
                                             .rows(rows());
    if (useV10) {
      builder.useV10();
    }
    return builder.buildMMappedIndex();
  }

  private static List<InputRow> rows()
  {
    final List<String> dimensions = List.of("dim", "longDim", "nested", "variant");
    return List.of(
        new MapBasedInputRow(
            1_000L,
            dimensions,
            Map.of(
                "dim", "alpha", "longDim", 7L,
                "nested", Map.of("customer", Map.of("region", "IN", "risk", 1.25), "ids", List.of(1L, 2L)),
                "variant", "first", "longVal", 100L, "doubleVal", 1.25
            )
        ),
        new MapBasedInputRow(
            2_000L,
            dimensions,
            Map.of(
                "dim", "beta", "longDim", 8L,
                "nested", Map.of("customer", Map.of("region", "EU", "risk", 2.5), "ids", List.of(3L)),
                "variant", 19L, "longVal", 200L, "doubleVal", 2.5
            )
        ),
        new MapBasedInputRow(
            3_000L,
            dimensions,
            Map.of(
                "dim", "gamma", "longDim", 9L,
                "nested", Map.of("customer", Map.of("region", "US", "risk", 0.75), "ids", List.of(4L, 5L)),
                "variant", 1.5D, "longVal", 300L, "doubleVal", 3.75
            )
        ),
        new MapBasedInputRow(
            4_000L,
            dimensions,
            Map.of("dim", "delta", "longDim", 10L, "variant", "last", "longVal", 400L, "doubleVal", 4.0)
        )
    );
  }
}
