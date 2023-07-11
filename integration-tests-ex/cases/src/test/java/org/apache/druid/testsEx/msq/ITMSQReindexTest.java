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

package org.apache.druid.testsEx.msq;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.commons.io.FilenameUtils;
import org.apache.curator.shaded.com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.testsEx.categories.MultiStageQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RunWith(DruidTestRunner.class)
@Category(MultiStageQuery.class)
public class ITMSQReindexTest extends AbstractITBatchIndexTest
{
  private static final String MSQ_TASKS_DIR = "/multi-stage-query/";

  private static final Logger LOG = new Logger(ITMSQReindexTest.class);

  public static List<List<String>> test_cases()
  {
    return Arrays.asList(
        Arrays.asList("wikipedia_index_msq.sql", "wikipedia_reindex_msq.sql", "wikipedia_reindex_queries.json"),
        Arrays.asList("wikipedia_merge_index_msq.sql", "wikipedia_merge_reindex_msq.sql", "wikipedia_merge_index_queries.json"),
        Arrays.asList("wikipedia_index_task_with_transform.sql", "wikipedia_reindex_with_transform_msq.sql", "wikipedia_reindex_queries_with_transforms.json")
    );
  }

  @Test
  @Parameters(method = "test_cases")
  @TestCaseName("Test_{index} ({0}, {1}, {2})")
  public void testMSQDruidInputSource(String sqlFileName, String reIndexSqlFileName, String reIndexQueryFileName)
  {
    String indexDatasource = FilenameUtils.removeExtension(sqlFileName);
    String reindexDatasource = FilenameUtils.removeExtension(reIndexSqlFileName);
    Map<String, Object> context = ImmutableMap.of(MultiStageQueryContext.CTX_FINALIZE_AGGREGATIONS, false,
                                                  MultiStageQueryContext.CTX_MAX_NUM_TASKS, 5,
                                                  GroupByQueryConfig.CTX_KEY_ENABLE_MULTI_VALUE_UNNESTING, false);
    try {
      submitMSQTaskFromFile(MSQ_TASKS_DIR + sqlFileName,
                            indexDatasource,
                            context);

      runReindexMSQTaskandTestQueries(MSQ_TASKS_DIR + reIndexSqlFileName,
                                      MSQ_TASKS_DIR + reIndexQueryFileName,
                                      indexDatasource,
                                      reindexDatasource,
                                      context);
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing [%s, %s, %s]", sqlFileName, reIndexSqlFileName, reIndexQueryFileName);
      throw new RuntimeException(e);
    }
  }
}
