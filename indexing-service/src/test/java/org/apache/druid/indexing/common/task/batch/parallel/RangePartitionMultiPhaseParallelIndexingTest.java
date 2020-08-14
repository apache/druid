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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.google.common.collect.SetMultimap;
import org.apache.druid.common.config.NullValueHandlingConfig;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CSVParseSpec;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DimensionsSpec;
import org.apache.druid.data.input.impl.ParseSpec;
import org.apache.druid.data.input.impl.TimestampSpec;
import org.apache.druid.indexer.TaskState;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.indexing.common.LockGranularity;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.guava.Comparators;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.apache.druid.timeline.partition.SingleDimensionShardSpec;
import org.hamcrest.Matchers;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.ProvideSystemProperty;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

@RunWith(Parameterized.class)
public class RangePartitionMultiPhaseParallelIndexingTest extends AbstractMultiPhaseParallelIndexingTest
{
  private static final boolean USE_INPUT_FORMAT_API = true;
  private static final boolean USE_MULTIVALUE_DIM = true;
  private static final int NUM_FILE = 10;
  private static final int NUM_ROW = 20;
  private static final int DIM_FILE_CARDINALITY = 2;
  private static final int NUM_PARTITION = 2;
  private static final int YEAR = 2017;
  private static final Interval INTERVAL_TO_INDEX = Intervals.of("%s-12/P1M", YEAR);
  private static final String TIME = "ts";
  private static final String DIM1 = "dim1";
  private static final String DIM2 = "dim2";
  private static final String LIST_DELIMITER = "|";
  private static final List<String> DIMS = ImmutableList.of(DIM1, DIM2);
  private static final String TEST_FILE_NAME_PREFIX = "test_";
  private static final TimestampSpec TIMESTAMP_SPEC = new TimestampSpec(TIME, "auto", null);
  private static final DimensionsSpec DIMENSIONS_SPEC = new DimensionsSpec(
      DimensionsSpec.getDefaultSchemas(Arrays.asList(TIME, DIM1, DIM2))
  );
  private static final ParseSpec PARSE_SPEC = new CSVParseSpec(
      TIMESTAMP_SPEC,
      DIMENSIONS_SPEC,
      LIST_DELIMITER,
      Arrays.asList(TIME, DIM1, DIM2, "val"),
      false,
      0
  );
  private static final InputFormat INPUT_FORMAT = new CsvInputFormat(
      Arrays.asList(TIME, DIM1, DIM2, "val"),
      LIST_DELIMITER,
      false,
      false,
      0
  );

  @Parameterized.Parameters(name = "{0}, useInputFormatApi={1}, maxNumConcurrentSubTasks={2}, useMultiValueDim={3}")
  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[]{LockGranularity.TIME_CHUNK, !USE_INPUT_FORMAT_API, 2, !USE_MULTIVALUE_DIM},
        new Object[]{LockGranularity.TIME_CHUNK, USE_INPUT_FORMAT_API, 2, !USE_MULTIVALUE_DIM},
        new Object[]{LockGranularity.SEGMENT, USE_INPUT_FORMAT_API, 2, !USE_MULTIVALUE_DIM},
        new Object[]{LockGranularity.SEGMENT, USE_INPUT_FORMAT_API, 1, !USE_MULTIVALUE_DIM},  // will spawn subtask
        new Object[]{LockGranularity.SEGMENT, USE_INPUT_FORMAT_API, 2, USE_MULTIVALUE_DIM}  // expected to fail
    );
  }

  // Interpret empty values in CSV as null
  @Rule
  public final ProvideSystemProperty noDefaultNullValue = new ProvideSystemProperty(
      NullValueHandlingConfig.NULL_HANDLING_CONFIG_STRING,
      "false"
  );

  private File inputDir;
  private SetMultimap<Interval, List<Object>> intervalToDims;

  private final int maxNumConcurrentSubTasks;
  private final boolean useMultivalueDim;

  public RangePartitionMultiPhaseParallelIndexingTest(
      LockGranularity lockGranularity,
      boolean useInputFormatApi,
      int maxNumConcurrentSubTasks,
      boolean useMultivalueDim
  )
  {
    super(lockGranularity, useInputFormatApi);
    this.maxNumConcurrentSubTasks = maxNumConcurrentSubTasks;
    this.useMultivalueDim = useMultivalueDim;
  }

  @Before
  public void setup() throws IOException
  {
    inputDir = temporaryFolder.newFolder("data");
    intervalToDims = createInputFiles(inputDir, useMultivalueDim);
  }

  private static SetMultimap<Interval, List<Object>> createInputFiles(File inputDir, boolean useMultivalueDim)
      throws IOException
  {
    SetMultimap<Interval, List<Object>> intervalToDims = HashMultimap.create();

    for (int fileIndex = 0; fileIndex < NUM_FILE; fileIndex++) {
      Path path = new File(inputDir, TEST_FILE_NAME_PREFIX + fileIndex).toPath();
      try (final Writer writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
        for (int i = 0; i < (NUM_ROW / DIM_FILE_CARDINALITY); i++) {
          for (int d = 0; d < DIM_FILE_CARDINALITY; d++) {
            int rowIndex = i * DIM_FILE_CARDINALITY + d;
            String dim1Value = createDim1Value(rowIndex, fileIndex, useMultivalueDim);

            // This is the original row
            writeRow(writer, i + d, dim1Value, fileIndex, intervalToDims);

            // This row should get rolled up with original row
            writeRow(writer, i + d, dim1Value, fileIndex, intervalToDims);

            // This row should not get rolled up with original row
            writeRow(writer, i + d, dim1Value, fileIndex + NUM_FILE, intervalToDims);
          }
        }
      }
    }

    return intervalToDims;
  }

  @Nullable
  private static String createDim1Value(int rowIndex, int fileIndex, boolean useMultivalueDim)
  {
    if (rowIndex == fileIndex) {
      return null;
    }

    String dim1Value = String.valueOf(fileIndex);
    return useMultivalueDim ? dim1Value + LIST_DELIMITER + dim1Value : dim1Value;
  }

  private static void writeRow(
      Writer writer,
      int day,
      @Nullable String dim1Value,
      int fileIndex,
      Multimap<Interval, List<Object>> intervalToDims
  ) throws IOException
  {
    Interval interval = Intervals.of("%s-12-%d/%s-12-%d", YEAR, day + 1, YEAR, day + 2);
    String startDate = interval.getStart().toString("y-M-d");
    String dim2Value = "test file " + fileIndex;
    String row = startDate + ",";
    if (dim1Value != null) {
      row += dim1Value;
    }
    row += "," + dim2Value + "\n";
    writer.write(row);
    intervalToDims.put(interval, Arrays.asList(dim1Value, dim2Value));
  }

  @Test
  public void createsCorrectRangePartitions() throws Exception
  {
    int targetRowsPerSegment = NUM_ROW * 2 / DIM_FILE_CARDINALITY / NUM_PARTITION;
    final Set<DataSegment> publishedSegments = runTestTask(
        new SingleDimensionPartitionsSpec(
            targetRowsPerSegment,
            null,
            DIM1,
            false
        ),
        useMultivalueDim ? TaskState.FAILED : TaskState.SUCCESS,
        false
    );

    if (!useMultivalueDim) {
      assertRangePartitions(publishedSegments);
    }
  }

  @Test
  public void testAppendLinearlyPartitionedSegmentsToHashPartitionedDatasourceSuccessfullyAppend()
  {
    if (useMultivalueDim) {
      return;
    }
    final int targetRowsPerSegment = NUM_ROW / DIM_FILE_CARDINALITY / NUM_PARTITION;
    final Set<DataSegment> publishedSegments = new HashSet<>();
    publishedSegments.addAll(
        runTestTask(
            new SingleDimensionPartitionsSpec(
                targetRowsPerSegment,
                null,
                DIM1,
                false
            ),
            TaskState.SUCCESS,
            false
        )
    );
    // Append
    publishedSegments.addAll(
        runTestTask(
            new DynamicPartitionsSpec(5, null),
            TaskState.SUCCESS,
            true
        )
    );
    // And append again
    publishedSegments.addAll(
        runTestTask(
            new DynamicPartitionsSpec(10, null),
            TaskState.SUCCESS,
            true
        )
    );

    final Map<Interval, List<DataSegment>> intervalToSegments = new HashMap<>();
    publishedSegments.forEach(
        segment -> intervalToSegments.computeIfAbsent(segment.getInterval(), k -> new ArrayList<>()).add(segment)
    );
    for (Entry<Interval, List<DataSegment>> entry : intervalToSegments.entrySet()) {
      final List<DataSegment> segments = entry.getValue();
      final List<DataSegment> rangedSegments = segments
          .stream()
          .filter(segment -> segment.getShardSpec().getClass() == SingleDimensionShardSpec.class)
          .collect(Collectors.toList());
      final List<DataSegment> linearSegments = segments
          .stream()
          .filter(segment -> segment.getShardSpec().getClass() == NumberedShardSpec.class)
          .collect(Collectors.toList());

      for (DataSegment rangedSegment : rangedSegments) {
        final SingleDimensionShardSpec rangeShardSpec = (SingleDimensionShardSpec) rangedSegment.getShardSpec();
        for (DataSegment linearSegment : linearSegments) {
          Assert.assertEquals(rangedSegment.getInterval(), linearSegment.getInterval());
          Assert.assertEquals(rangedSegment.getVersion(), linearSegment.getVersion());
          final NumberedShardSpec numberedShardSpec = (NumberedShardSpec) linearSegment.getShardSpec();
          Assert.assertEquals(rangeShardSpec.getNumCorePartitions(), numberedShardSpec.getNumCorePartitions());
          Assert.assertTrue(rangeShardSpec.getPartitionNum() < numberedShardSpec.getPartitionNum());
        }
      }
    }
  }

  private Set<DataSegment> runTestTask(
      PartitionsSpec partitionsSpec,
      TaskState expectedTaskState,
      boolean appendToExisting
  )
  {
    if (isUseInputFormatApi()) {
      return runTestTask(
          TIMESTAMP_SPEC,
          DIMENSIONS_SPEC,
          INPUT_FORMAT,
          null,
          INTERVAL_TO_INDEX,
          inputDir,
          TEST_FILE_NAME_PREFIX + "*",
          partitionsSpec,
          maxNumConcurrentSubTasks,
          expectedTaskState,
          appendToExisting
      );
    } else {
      return runTestTask(
          null,
          null,
          null,
          PARSE_SPEC,
          INTERVAL_TO_INDEX,
          inputDir,
          TEST_FILE_NAME_PREFIX + "*",
          partitionsSpec,
          maxNumConcurrentSubTasks,
          expectedTaskState,
          appendToExisting
      );
    }
  }

  private void assertRangePartitions(Set<DataSegment> publishedSegments) throws IOException
  {
    Multimap<Interval, DataSegment> intervalToSegments = ArrayListMultimap.create();
    publishedSegments.forEach(s -> intervalToSegments.put(s.getInterval(), s));

    Set<Interval> publishedIntervals = intervalToSegments.keySet();
    assertHasExpectedIntervals(publishedIntervals);

    File tempSegmentDir = temporaryFolder.newFolder();

    intervalToSegments.asMap().forEach((interval, segments) -> {
      assertNumPartition(segments);

      List<String> allValues = new ArrayList<>(NUM_ROW);
      for (DataSegment segment : segments) {
        List<String> values = getColumnValues(segment, tempSegmentDir);
        assertValuesInRange(values, segment);
        allValues.addAll(values);
      }

      assertIntervalHasAllExpectedValues(interval, allValues);
    });
  }

  private void assertHasExpectedIntervals(Set<Interval> publishedSegmentIntervals)
  {
    Assert.assertEquals(intervalToDims.keySet(), publishedSegmentIntervals);
  }

  private static void assertNumPartition(Collection<DataSegment> segments)
  {
    Assert.assertEquals(NUM_PARTITION, segments.size());
  }

  private List<String> getColumnValues(DataSegment segment, File tempDir)
  {
    List<ScanResultValue> results = querySegment(segment, DIMS, tempDir);
    Assert.assertEquals(1, results.size());
    List<LinkedHashMap<String, String>> rows = (List<LinkedHashMap<String, String>>) results.get(0).getEvents();
    return rows.stream()
               .map(row -> row.get(DIM1))
               .collect(Collectors.toList());
  }

  private static void assertValuesInRange(List<String> values, DataSegment segment)
  {
    SingleDimensionShardSpec shardSpec = (SingleDimensionShardSpec) segment.getShardSpec();
    String start = shardSpec.getStart();
    String end = shardSpec.getEnd();
    Assert.assertTrue(shardSpec.toString(), start != null || end != null);

    for (String value : values) {
      if (start != null) {
        Assert.assertThat(value.compareTo(start), Matchers.greaterThanOrEqualTo(0));
      }

      if (end != null) {
        if (value == null) {
          Assert.assertNull("null values should be in first partition", start);
        } else {
          Assert.assertThat(value.compareTo(end), Matchers.lessThan(0));
        }
      }
    }
  }

  private void assertIntervalHasAllExpectedValues(Interval interval, List<String> actualValues)
  {
    List<String> expectedValues = intervalToDims.get(interval)
                                                .stream()
                                                .map(d -> (String) d.get(0))
                                                .sorted(Comparators.naturalNullsFirst())
                                                .collect(Collectors.toList());
    actualValues.sort(Comparators.naturalNullsFirst());
    Assert.assertEquals(interval.toString(), expectedValues, actualValues);
  }
}
