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
import com.google.inject.Inject;
import org.apache.commons.io.IOUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.testing.utils.ITRetryUtil;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Test(groups = {TestNGGroup.COMPACTION, TestNGGroup.QUICKSTART_COMPATIBLE})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITCompactionSparseColumnTest extends AbstractIndexerTest
{
  private static final String INDEX_DATASOURCE = "sparse_column_index_test";

  private static final String INDEX_TASK = "/indexer/sparse_column_index_task.json";
  private static final String COMPACTION_QUERIES_RESOURCE = "/indexer/sparse_column_index_queries.json";

  private static final String COMPACTION_TASK_WITHOUT_DIMENSION = "/indexer/sparse_column_without_dim_compaction_task.json";
  private static final String COMPACTION_TASK_WITH_DIMENSION = "/indexer/sparse_column_with_dim_compaction_task.json";

  @Inject
  private IntegrationTestingConfig config;

  private String fullDatasourceName;

  @BeforeMethod
  public void setFullDatasourceName(Method method)
  {
    fullDatasourceName = INDEX_DATASOURCE + config.getExtraDatasourceNameSuffix() + "-" + method.getName();
  }

  @Test
  public void testCompactionPerfectRollUpWithoutDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn(fullDatasourceName);
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      String template = getResourceAsString(COMPACTION_TASK_WITHOUT_DIMENSION);
      template = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);
      final String taskID = indexer.submitTask(template);
      indexer.waitUntilTaskCompletes(taskID);
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(fullDatasourceName),
          "Segment Compaction"
      );
      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimB", "dimA", "dimC", "dimD", "dimE", "dimF"
      // (This is the same as the ordering in the initial ingestion task).
      List<List<Object>> segmentRows = ImmutableList.of(
          Arrays.asList(1442016000000L, "F", "C", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "J", "C", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "R", "J", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "S", "Z", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "T", "H", null, null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "X", null, "A", null, null, null, 1, 1),
          Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 3, 3),
          Arrays.asList(1442016000000L, "Z", "H", null, null, null, null, 1, 1)
      );
      verifyCompactedData(segmentRows);
    }
  }

  @Test
  public void testCompactionPerfectRollUpWithLexicographicDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn(fullDatasourceName);
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      String template = getResourceAsString(COMPACTION_TASK_WITH_DIMENSION);
      template = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);
      //
      List<String> dimensionsOrder = ImmutableList.of("dimA", "dimB", "dimC");
      template = StringUtils.replace(
          template,
          "%%DIMENSION_NAMES%%",
          jsonMapper.writeValueAsString(dimensionsOrder)
      );
      final String taskID = indexer.submitTask(template);
      indexer.waitUntilTaskCompletes(taskID);
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(fullDatasourceName),
          "Segment Compaction"
      );
      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimA", "dimB", "dimC"
      List<List<Object>> segmentRows = ImmutableList.of(
          Arrays.asList(1442016000000L, null, "X", "A", 1, 1),
          Arrays.asList(1442016000000L, "C", "F", null, 1, 1),
          Arrays.asList(1442016000000L, "C", "J", null, 1, 1),
          Arrays.asList(1442016000000L, "H", "T", null, 1, 1),
          Arrays.asList(1442016000000L, "H", "X", null, 3, 3),
          Arrays.asList(1442016000000L, "H", "Z", null, 1, 1),
          Arrays.asList(1442016000000L, "J", "R", null, 1, 1),
          Arrays.asList(1442016000000L, "Z", "S", null, 1, 1)
      );
      verifyCompactedData(segmentRows);
    }
  }

  @Test
  public void testCompactionPerfectRollUpWithNonLexicographicDimensionSpec() throws Exception
  {
    try (final Closeable ignored = unloader(fullDatasourceName)) {
      // Load and verify initial data
      loadAndVerifyDataWithSparseColumn(fullDatasourceName);
      // Compaction with perfect roll up. Rolls with "X", "H" (for the first and second columns respectively) should be roll up
      String template = getResourceAsString(COMPACTION_TASK_WITH_DIMENSION);
      template = StringUtils.replace(template, "%%DATASOURCE%%", fullDatasourceName);
      //
      List<String> dimensionsOrder = ImmutableList.of("dimC", "dimB", "dimA");
      template = StringUtils.replace(
          template,
          "%%DIMENSION_NAMES%%",
          jsonMapper.writeValueAsString(dimensionsOrder)
      );
      final String taskID = indexer.submitTask(template);
      indexer.waitUntilTaskCompletes(taskID);
      ITRetryUtil.retryUntilTrue(
          () -> coordinator.areSegmentsLoaded(fullDatasourceName),
          "Segment Compaction"
      );
      // Verify compacted data.
      // Compacted data only have one segments. First segment have the following rows:
      // The ordering of the columns will be "dimC", "dimB", "dimA"
      List<List<Object>> segment1Rows = ImmutableList.of(
          Arrays.asList(1442016000000L, null, "F", "C", 1, 1),
          Arrays.asList(1442016000000L, null, "J", "C", 1, 1),
          Arrays.asList(1442016000000L, null, "R", "J", 1, 1),
          Arrays.asList(1442016000000L, null, "S", "Z", 1, 1),
          Arrays.asList(1442016000000L, null, "T", "H", 1, 1),
          Arrays.asList(1442016000000L, null, "X", "H", 3, 3),
          Arrays.asList(1442016000000L, null, "Z", "H", 1, 1),
          Arrays.asList(1442016000000L, "A", "X", null, 1, 1)
      );
      verifyCompactedData(segment1Rows);
    }
  }

  private void loadAndVerifyDataWithSparseColumn(String fullDatasourceName) throws Exception
  {
    loadData(INDEX_TASK, fullDatasourceName);
    List<Map<String, List<List<Object>>>> expectedResultBeforeCompaction = new ArrayList<>();
    // First segments have the following rows:
    List<List<Object>> segment1Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "F", "C", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "J", "C", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment1Rows));
    // Second segments have the following rows:
    List<List<Object>> segment2Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "S", "Z", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "Z", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment2Rows));
    // Third segments have the following rows:
    List<List<Object>> segment3Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "R", "J", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "T", "H", null, null, null, null, 1, 1),
        Arrays.asList(1442016000000L, "X", "H", null, null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment3Rows));
    // Fourth segments have the following rows:
    List<List<Object>> segment4Rows = ImmutableList.of(
        Arrays.asList(1442016000000L, "X", null, "A", null, null, null, 1, 1)
    );
    expectedResultBeforeCompaction.add(ImmutableMap.of("events", segment4Rows));
    verifyQueryResult(expectedResultBeforeCompaction, 10, 10, 1);
  }

  private void verifyCompactedData(List<List<Object>> segmentRows) throws Exception
  {
    List<Map<String, List<List<Object>>>> expectedResultAfterCompaction = new ArrayList<>();
    expectedResultAfterCompaction.add(ImmutableMap.of("events", segmentRows));
    verifyQueryResult(expectedResultAfterCompaction, 8, 10, 0.8);
  }

  private void verifyQueryResult(
      List<Map<String, List<List<Object>>>> expectedScanResult,
      int expectedNumRoll,
      int expectedSumCount,
      double expectedRollupRatio
  ) throws Exception
  {
    InputStream is = AbstractITBatchIndexTest.class.getResourceAsStream(COMPACTION_QUERIES_RESOURCE);
    String queryResponseTemplate = IOUtils.toString(is, StandardCharsets.UTF_8);
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%DATASOURCE%%",
        fullDatasourceName
    );
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%EXPECTED_SCAN_RESULT%%",
        jsonMapper.writeValueAsString(expectedScanResult)
    );
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%EXPECTED_SUM_COUNT%%",
        jsonMapper.writeValueAsString(expectedSumCount)
    );
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%EXPECTED_ROLLUP_RATIO%%",
        jsonMapper.writeValueAsString(expectedRollupRatio)
    );
    queryResponseTemplate = StringUtils.replace(
        queryResponseTemplate,
        "%%EXPECTED_NUM_ROW%%",
        jsonMapper.writeValueAsString(expectedNumRoll)
    );
    queryHelper.testQueriesFromString(queryResponseTemplate);
  }
}
