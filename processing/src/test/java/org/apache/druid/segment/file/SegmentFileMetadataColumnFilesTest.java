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

package org.apache.druid.segment.file;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.MapBasedInputRow;
import org.apache.druid.data.input.impl.AggregateProjectionSpec;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.segment.AutoTypeColumnSchema;
import org.apache.druid.segment.IndexBuilder;
import org.apache.druid.segment.IndexIO;
import org.apache.druid.segment.IndexSpec;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.segment.data.CompressionStrategy;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.Projections;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Tests that {@link SegmentFileMetadata#getColumnFiles()} recorded by {@link SegmentFileBuilderV10} during a real
 * merge via {@link org.apache.druid.segment.IndexMergerV10} correctly attributes every internal file to its owning
 * column, including multi-file serde layouts (nested/auto columns) and projection columns.
 */
class SegmentFileMetadataColumnFilesTest extends InitializedNullHandlingTest
{
  private static final ObjectMapper JSON_MAPPER = TestHelper.makeJsonMapper();
  private static final DateTime TIME = DateTimes.of("2025-01-01");
  private static final String BASE_PREFIX = Projections.BASE_TABLE_PROJECTION_NAME + "/";
  private static final String PROJECTION_NAME = "dim1_hourly_metric1_sum";

  @TempDir
  static File sharedTempDir;

  private static SegmentFileMetadata metadata;

  @BeforeAll
  static void buildSegment() throws IOException
  {
    final File tmpDir = new File(sharedTempDir, "build_" + ThreadLocalRandom.current().nextInt());
    final List<InputRow> rows = List.of(
        row("a", 1L, Map.of("x", 1L, "y", "one")),
        row("a", 2L, Map.of("x", 2L, "y", "two")),
        row("b", 3L, Map.of("x", 3L, "y", "three")),
        row("b", 4L, Map.of("x", 4L, "y", "four"))
    );
    final File segmentDir =
        IndexBuilder.create()
                    .useV10()
                    .tmpDir(tmpDir)
                    .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                    .schema(
                        IncrementalIndexSchema.builder()
                                              .withDimensionsSpec(
                                                  DimensionsSpec.builder()
                                                                .setDimensions(
                                                                    List.of(
                                                                        new StringDimensionSchema("dim1"),
                                                                        new AutoTypeColumnSchema("nest", null, null),
                                                                        new LongDimensionSchema("metric1"),
                                                                        // declared but never fed a value: merges as
                                                                        // an all-null column with a zero-length file
                                                                        new StringDimensionSchema("allNull")
                                                                    )
                                                                )
                                                                .build()
                                              )
                                              .withRollup(false)
                                              .withMinTimestamp(TIME.getMillis())
                                              .withProjections(
                                                  List.of(
                                                      AggregateProjectionSpec
                                                          .builder(PROJECTION_NAME)
                                                          .virtualColumns(
                                                              Granularities.toVirtualColumn(
                                                                  Granularities.HOUR,
                                                                  Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                                              )
                                                          )
                                                          .groupingColumns(
                                                              new StringDimensionSchema("dim1"),
                                                              new LongDimensionSchema(
                                                                  Granularities.GRANULARITY_VIRTUAL_COLUMN_NAME
                                                              )
                                                          )
                                                          .aggregators(
                                                              new LongSumAggregatorFactory("_metric1_sum", "metric1"),
                                                              new CountAggregatorFactory("_count")
                                                          )
                                                          .build()
                                                  )
                                              )
                                              .build()
                    )
                    .indexSpec(IndexSpec.builder().withMetadataCompression(CompressionStrategy.ZSTD).build())
                    .rows(rows)
                    .buildMMappedIndexFile();

    final File segmentFile = new File(segmentDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(segmentFile, JSON_MAPPER)) {
      metadata = mapper.getSegmentFileMetadata();
    }
  }

  private static InputRow row(String dim1, long metric1, Map<String, Object> nest)
  {
    return new MapBasedInputRow(
        TIME.plusMinutes((int) metric1),
        List.of("dim1", "nest", "metric1"),
        Map.of("dim1", dim1, "nest", nest, "metric1", metric1)
    );
  }

  @Test
  void testPlainColumnsHaveExactlyTheirOwnFile()
  {
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    Assertions.assertEquals(List.of(BASE_PREFIX + "dim1"), columnFiles.get(BASE_PREFIX + "dim1"));
    Assertions.assertEquals(List.of(BASE_PREFIX + "metric1"), columnFiles.get(BASE_PREFIX + "metric1"));
    Assertions.assertEquals(List.of(BASE_PREFIX + "__time"), columnFiles.get(BASE_PREFIX + "__time"));
  }

  @Test
  void testNestedColumnListsEverySubFile()
  {
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    final List<String> nestFiles = columnFiles.get(BASE_PREFIX + "nest");
    Assertions.assertNotNull(nestFiles);

    // main file first, then the serde's sub-files in write order
    Assertions.assertEquals(BASE_PREFIX + "nest", nestFiles.get(0));
    Assertions.assertTrue(
        nestFiles.stream().anyMatch(f -> f.contains(".__field_")),
        "expected per-field files in " + nestFiles
    );

    // the list is complete: every metadata file under the nested column's dotted namespace is attributed to it
    final Set<String> expected = new TreeSet<>();
    for (String file : metadata.getFiles().keySet()) {
      if (file.equals(BASE_PREFIX + "nest") || file.startsWith(BASE_PREFIX + "nest.")) {
        expected.add(file);
      }
    }
    Assertions.assertEquals(expected, new TreeSet<>(nestFiles));
  }

  @Test
  void testAllNullColumnZeroLengthFileIsAttributed()
  {
    // an all-null column serializes zero payload bytes (NullColumnPartSerde: the descriptor stored in the metadata
    // header is its entire representation), leaving a zero-length internal file that must still be recorded and
    // attributed like any other so partial-download planning (which must handle zero-length spans) sees it
    final SegmentInternalFileMetadata fileMeta = metadata.getFiles().get(BASE_PREFIX + "allNull");
    Assertions.assertNotNull(fileMeta);
    Assertions.assertEquals(0, fileMeta.getSize());

    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    Assertions.assertEquals(List.of(BASE_PREFIX + "allNull"), columnFiles.get(BASE_PREFIX + "allNull"));
  }

  @Test
  void testEveryInternalFileIsAttributedToExactlyOneColumn()
  {
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    final Set<String> attributed = new HashSet<>();
    for (Map.Entry<String, List<String>> entry : columnFiles.entrySet()) {
      for (String file : entry.getValue()) {
        Assertions.assertTrue(attributed.add(file), "file[" + file + "] attributed to more than one column");
      }
    }
    Assertions.assertEquals(metadata.getFiles().keySet(), attributed);
  }

  @Test
  void testProjectionColumnFilesStayInProjectionNamespace()
  {
    // projection columns that reuse the parent (base table) column's dictionary don't write those parent files, so
    // their lists must only name files under the projection's own prefix
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    final String projectionPrefix = PROJECTION_NAME + "/";
    boolean sawProjectionColumn = false;
    for (Map.Entry<String, List<String>> entry : columnFiles.entrySet()) {
      if (entry.getKey().startsWith(projectionPrefix)) {
        sawProjectionColumn = true;
        for (String file : entry.getValue()) {
          Assertions.assertTrue(
              file.startsWith(projectionPrefix),
              "projection column[" + entry.getKey() + "] lists out-of-namespace file[" + file + "]"
          );
        }
      }
    }
    Assertions.assertTrue(sawProjectionColumn, "no projection columns recorded in " + columnFiles.keySet());
  }

  @Test
  void testColumnFileNamesAreInternedAgainstFileMapKeys()
  {
    final Map<String, List<String>> columnFiles = metadata.getColumnFiles();
    Assertions.assertNotNull(columnFiles);
    for (List<String> files : columnFiles.values()) {
      for (String file : files) {
        for (String key : metadata.getFiles().keySet()) {
          if (key.equals(file)) {
            Assertions.assertSame(key, file, "file name[" + file + "] not interned");
          }
        }
      }
    }
  }

  @Test
  void testJsonRoundTripPreservesColumnFiles() throws IOException
  {
    final byte[] json = JSON_MAPPER.writeValueAsBytes(metadata);
    final SegmentFileMetadata roundTripped = JSON_MAPPER.readValue(json, SegmentFileMetadata.class);
    Assertions.assertEquals(metadata.getColumnFiles(), roundTripped.getColumnFiles());
  }

  @Test
  void testDeserializeWithoutColumnFilesYieldsNull() throws IOException
  {
    final ObjectNode node = (ObjectNode) JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsBytes(metadata));
    node.remove("columnFiles");
    final SegmentFileMetadata stripped = JSON_MAPPER.treeToValue(node, SegmentFileMetadata.class);
    Assertions.assertNull(stripped.getColumnFiles());
    Assertions.assertEquals(metadata.getFiles().keySet(), stripped.getFiles().keySet());
  }

  @Test
  void testColumnFilesReferencingMissingFileThrows()
  {
    // fail-fast consistency check: every file a columnFiles entry names must resolve in the files map, since a
    // dangling reference would otherwise silently plan no download and only fail much later at deserialization
    final DruidException thrown = Assertions.assertThrows(
        DruidException.class,
        () -> new SegmentFileMetadata(
            metadata.getContainers(),
            metadata.getFiles(),
            metadata.getInterval(),
            metadata.getColumnDescriptors(),
            Map.of(BASE_PREFIX + "dim1", List.of(BASE_PREFIX + "dim1", BASE_PREFIX + "dim1.__does_not_exist")),
            metadata.getProjections(),
            metadata.getBitmapEncoding()
        )
    );
    Assertions.assertTrue(
        thrown.getMessage().contains("__does_not_exist"),
        "unexpected message: " + thrown.getMessage()
    );
  }

  @Test
  void testDeserializeToleratesUnknownFields() throws IOException
  {
    // old readers must silently drop fields added after their time; verify the mapper configuration this relies on
    final ObjectNode node = (ObjectNode) JSON_MAPPER.readTree(JSON_MAPPER.writeValueAsBytes(metadata));
    node.put("someFutureField", "someFutureValue");
    final SegmentFileMetadata parsed = JSON_MAPPER.treeToValue(node, SegmentFileMetadata.class);
    Assertions.assertEquals(metadata.getFiles().keySet(), parsed.getFiles().keySet());
  }
}
