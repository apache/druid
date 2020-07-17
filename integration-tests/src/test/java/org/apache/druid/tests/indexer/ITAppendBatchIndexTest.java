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

package org.apache.druid.tests.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.indexer.partitions.HashedPartitionsSpec;
import org.apache.druid.indexer.partitions.PartitionsSpec;
import org.apache.druid.indexer.partitions.SecondaryPartitionType;
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.apache.druid.timeline.DataSegment;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = {TestNGGroup.OTHER_INDEX})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAppendBatchIndexTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITAppendBatchIndexTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String INDEX_QUERIES_INITIAL_INGESTION_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_QUERIES_POST_APPEND_PRE_COMPACT_RESOURCE = "/indexer/wikipedia_double_without_roll_up_index_queries.json";
  private static final String INDEX_QUERIES_POST_APPEND_POST_COMPACT_RESOURCE = "/indexer/wikipedia_double_with_roll_up_index_queries.json";

  private static final String COMPACTION_TASK = "/indexer/wikipedia_compaction_task.json";

  @DataProvider()
  public static Object[][] resources()
  {
    return new Object[][]{
        // First index with dynamically-partitioned then append dynamically-partitioned
        {
          ImmutableList.of(
            new DynamicPartitionsSpec(null, null),
            new DynamicPartitionsSpec(null, null)
          ),
          ImmutableList.of(4, 8, 2)
        },
        // First index with hash-partitioned then append dynamically-partitioned
        {
          ImmutableList.of(
            new HashedPartitionsSpec(null, 3, ImmutableList.of("page", "user")),
            new DynamicPartitionsSpec(null, null)
          ),
          ImmutableList.of(6, 10, 2)
        },
        // First index with range-partitioned then append dynamically-partitioned
        {
          ImmutableList.of(
            new SingleDimensionPartitionsSpec(1000, null, "page", false),
            new DynamicPartitionsSpec(null, null)
          ),
          ImmutableList.of(2, 6, 2)
        }
    };
  }

  @Test(dataProvider = "resources")
  public void doIndexTest(List<PartitionsSpec> partitionsSpecList, List<Integer> expectedSegmentCountList) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      // Submit initial ingestion task
      submitIngestionTaskAndVerify(indexDatasource, partitionsSpecList.get(0), false);
      verifySegmentsCountAndLoaded(indexDatasource, expectedSegmentCountList.get(0));
      doTestQuery(indexDatasource, INDEX_QUERIES_INITIAL_INGESTION_RESOURCE, 2);
      // Submit append ingestion task
      submitIngestionTaskAndVerify(indexDatasource, partitionsSpecList.get(1), true);
      verifySegmentsCountAndLoaded(indexDatasource, expectedSegmentCountList.get(1));
      doTestQuery(indexDatasource, INDEX_QUERIES_POST_APPEND_PRE_COMPACT_RESOURCE, 2);
      // Submit compaction task
      final List<String> intervalsBeforeCompaction = coordinator.getSegmentIntervals(indexDatasource);
      intervalsBeforeCompaction.sort(null);
      compactData(indexDatasource);
      // Verification post compaction
      checkCompactionIntervals(indexDatasource, intervalsBeforeCompaction);
      verifySegmentsCountAndLoaded(indexDatasource, expectedSegmentCountList.get(2));
      verifySegmentsCompacted(indexDatasource, expectedSegmentCountList.get(2));
      doTestQuery(indexDatasource, INDEX_QUERIES_POST_APPEND_POST_COMPACT_RESOURCE, 2);
    }
  }

  private void submitIngestionTaskAndVerify(
      String indexDatasource,
      PartitionsSpec partitionsSpec,
      boolean appendToExisting
  ) throws Exception
  {
    InputFormatDetails inputFormatDetails = InputFormatDetails.JSON;
    Map inputFormatMap = new ImmutableMap.Builder<String, Object>().put("type", inputFormatDetails.getInputFormatType())
                                                                   .build();
    final Function<String, String> sqlInputSourcePropsTransform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%PARTITIONS_SPEC%%",
            jsonMapper.writeValueAsString(partitionsSpec)
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_FILTER%%",
            "*" + inputFormatDetails.getFileExtension()
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_BASE_DIR%%",
            "/resources/data/batch_index" + inputFormatDetails.getFolderSuffix()
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_FORMAT%%",
            jsonMapper.writeValueAsString(inputFormatMap)
        );
        spec = StringUtils.replace(
            spec,
            "%%APPEND_TO_EXISTING%%",
            jsonMapper.writeValueAsString(appendToExisting)
        );
        if (partitionsSpec instanceof DynamicPartitionsSpec) {
          spec = StringUtils.replace(
              spec,
              "%%FORCE_GUARANTEED_ROLLUP%%",
              jsonMapper.writeValueAsString(false)
          );
        } else if (partitionsSpec instanceof HashedPartitionsSpec || partitionsSpec instanceof SingleDimensionPartitionsSpec) {
          spec = StringUtils.replace(
              spec,
              "%%FORCE_GUARANTEED_ROLLUP%%",
              jsonMapper.writeValueAsString(true)
          );
        }
        return spec;
      }
      catch (Exception e) {
        throw new RuntimeException(e);
      }
    };

    doIndexTest(
        indexDatasource,
        INDEX_TASK,
        sqlInputSourcePropsTransform,
        null,
        false,
        false,
        true
    );
  }

  private void compactData(String fullDatasourceName) throws Exception
  {
    final String template = getResourceAsString(COMPACTION_TASK);
    String taskSpec = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);

    final String taskID = indexer.submitTask(taskSpec);
    LOG.info("TaskID for compaction task %s", taskID);
    indexer.waitUntilTaskCompletes(taskID);

    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment Compaction"
    );
  }

  private void verifySegmentsCountAndLoaded(String fullDatasourceName, int numExpectedSegments)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          int metadataSegmentCount = coordinator.getSegments(fullDatasourceName).size();
          LOG.info("Current metadata segment count: %d, expected: %d", metadataSegmentCount, numExpectedSegments);
          return metadataSegmentCount == numExpectedSegments;
        },
        "Segment count check"
    );
    ITRetryUtil.retryUntilTrue(
        () -> coordinator.areSegmentsLoaded(fullDatasourceName),
        "Segment load check"
    );
  }

  private void checkCompactionIntervals(String fullDatasourceName, List<String> expectedIntervals)
  {
    ITRetryUtil.retryUntilTrue(
        () -> {
          final List<String> actualIntervals = coordinator.getSegmentIntervals(fullDatasourceName);
          actualIntervals.sort(null);
          return actualIntervals.equals(expectedIntervals);
        },
        "Compaction interval check"
    );
  }

  private void verifySegmentsCompacted(String fullDatasourceName, int expectedCompactedSegmentCount)
  {
    List<DataSegment> segments = coordinator.getFullSegmentsMetadata(fullDatasourceName);
    List<DataSegment> foundCompactedSegments = new ArrayList<>();
    for (DataSegment segment : segments) {
      if (segment.getLastCompactionState() != null) {
        foundCompactedSegments.add(segment);
      }
    }
    Assert.assertEquals(foundCompactedSegments.size(), expectedCompactedSegmentCount);
    for (DataSegment compactedSegment : foundCompactedSegments) {
      Assert.assertNotNull(compactedSegment.getLastCompactionState());
      Assert.assertNotNull(compactedSegment.getLastCompactionState().getPartitionsSpec());
      Assert.assertEquals(compactedSegment.getLastCompactionState().getPartitionsSpec().getType(),
                          SecondaryPartitionType.LINEAR
      );
    }
  }
}
