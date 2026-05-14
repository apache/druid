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

import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.ListBasedInputRow;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.LongDimensionSchema;
import org.apache.druid.data.input.impl.StringDimensionSchema;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.file.SegmentFileMapperV10;
import org.apache.druid.segment.file.SegmentFileMetadata;
import org.apache.druid.segment.incremental.IncrementalIndexSchema;
import org.apache.druid.segment.projections.ProjectionMetadata;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.util.Arrays;
import java.util.List;

class IndexMergerV10MinMaxTimeTest extends InitializedNullHandlingTest
{
  @TempDir
  File tempDir;

  @Test
  void testMinMaxTimePersistedForTimeSortedSegment() throws Exception
  {
    // Default forceSegmentSortByTime=true: __time is implicitly first in the sort order, so the segment is sorted
    // ascending by __time. The merge loop should still capture min/max via timestampSelector.
    final DateTime base = DateTimes.of("2025-01-01");
    final RowSignature signature = RowSignature.builder()
                                               .add("dim", ColumnType.STRING)
                                               .add("metric", ColumnType.LONG)
                                               .build();
    final List<InputRow> rows = Arrays.asList(
        new ListBasedInputRow(signature, base, signature.getColumnNames(), Arrays.asList("a", 1L)),
        new ListBasedInputRow(signature, base.plusMinutes(5), signature.getColumnNames(), Arrays.asList("b", 2L)),
        new ListBasedInputRow(signature, base.plusMinutes(10), signature.getColumnNames(), Arrays.asList("c", 3L)),
        new ListBasedInputRow(signature, base.plusMinutes(20), signature.getColumnNames(), Arrays.asList("d", 4L))
    );

    final File segmentDir = buildV10Segment(
        rows,
        DimensionsSpec.builder()
                      .setDimensions(
                          List.of(
                              new StringDimensionSchema("dim"),
                              new LongDimensionSchema("metric")
                          )
                      )
                      .build()
    );

    assertBaseProjectionMinMaxTime(segmentDir, base.getMillis(), base.plusMinutes(20).getMillis());
  }

  @Test
  void testMinMaxTimePersistedForNonTimeSortedSegment() throws Exception
  {
    // forceSegmentSortByTime=false: the segment is sorted by the explicit dimension order (dim, then __time), so
    // min/max time do NOT correspond to the first/last row positions. The merge loop must capture min/max from the
    // timestampSelector regardless of physical row order.
    final DateTime base = DateTimes.of("2025-01-01");
    final RowSignature signature = RowSignature.builder()
                                               .add("dim", ColumnType.STRING)
                                               .build();
    final List<InputRow> rows = Arrays.asList(
        new ListBasedInputRow(signature, base.plusMinutes(20), signature.getColumnNames(), List.of("c")),
        new ListBasedInputRow(signature, base, signature.getColumnNames(), List.of("a")),
        new ListBasedInputRow(signature, base.plusMinutes(10), signature.getColumnNames(), List.of("b"))
    );

    final File segmentDir = buildV10Segment(
        rows,
        DimensionsSpec.builder()
                      .setDimensions(
                          List.of(
                              new StringDimensionSchema("dim"),
                              new LongDimensionSchema("__time")
                          )
                      )
                      .setForceSegmentSortByTime(false)
                      .build()
    );

    assertBaseProjectionMinMaxTime(segmentDir, base.getMillis(), base.plusMinutes(20).getMillis());
  }

  private File buildV10Segment(List<InputRow> rows, DimensionsSpec dimensionsSpec)
  {
    final long minTs = rows.stream().mapToLong(InputRow::getTimestampFromEpoch).min().orElseThrow();
    return IndexBuilder.create()
                       .useV10()
                       .tmpDir(tempDir)
                       .segmentWriteOutMediumFactory(OffHeapMemorySegmentWriteOutMediumFactory.instance())
                       .schema(
                           IncrementalIndexSchema.builder()
                                                 .withDimensionsSpec(dimensionsSpec)
                                                 .withRollup(false)
                                                 .withMinTimestamp(minTs)
                                                 .build()
                       )
                       .rows(rows)
                       .buildMMappedIndexFile();
  }

  private void assertBaseProjectionMinMaxTime(File segmentDir, long expectedMin, long expectedMax) throws Exception
  {
    final File v10File = new File(segmentDir, IndexIO.V10_FILE_NAME);
    try (SegmentFileMapperV10 mapper = SegmentFileMapperV10.create(v10File, TestHelper.JSON_MAPPER)) {
      final SegmentFileMetadata metadata = mapper.getSegmentFileMetadata();
      Assertions.assertNotNull(metadata.getProjections());
      Assertions.assertFalse(metadata.getProjections().isEmpty());
      // Base table is always the first projection in V10 metadata.
      final ProjectionMetadata baseProjection = metadata.getProjections().get(0);
      Assertions.assertEquals(Long.valueOf(expectedMin), baseProjection.getMinTime(), "minTime");
      Assertions.assertEquals(Long.valueOf(expectedMax), baseProjection.getMaxTime(), "maxTime");
    }
  }
}
