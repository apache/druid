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
import org.apache.druid.testsEx.categories.MultiStageQuery;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractITBatchIndexTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.List;

@RunWith(DruidTestRunner.class)
@Category(MultiStageQuery.class)
public class ITSQLBasedBatchIngestionTest extends AbstractITBatchIndexTest
{
  private static final String MSQ_TASKS_DIR = "/multi-stage-query/";

  private static final Logger LOG = new Logger(ITSQLBasedBatchIngestionTest.class);

  public static List<List<String>> test_cases()
  {
    return Arrays.asList(
        Arrays.asList("msq_inline.sql", "json_path_index_queries.json"),
        Arrays.asList("sparse_column_msq.sql", "sparse_column_msq.json"),
        Arrays.asList("wikipedia_http_inputsource_msq.sql", "wikipedia_http_inputsource_queries.json"),
        Arrays.asList("wikipedia_index_msq.sql", "wikipedia_index_queries.json"),
        Arrays.asList("wikipedia_merge_index_msq.sql", "wikipedia_merge_index_queries.json"),
        Arrays.asList("wikipedia_index_task_with_transform.sql", "wikipedia_index_queries_with_transform.json")
    );

  }

  @Test
  @Parameters(method = "test_cases")
  @TestCaseName("Test_{index} ({0}, {1})")
  public void testSQLBasedBatchIngestion(String sqlFileName, String queryFileName)
  {
    try {
      runMSQTaskandTestQueries(
          MSQ_TASKS_DIR + sqlFileName,
          MSQ_TASKS_DIR + queryFileName,
          FilenameUtils.removeExtension(sqlFileName),
          ImmutableMap.of("finalizeAggregations", false,
                          "maxNumTasks", 5,
                          "groupByEnableMultiValueUnnesting", false));
    }
    catch (Exception e) {
      LOG.error(e, "Error while testing [%s, %s]", sqlFileName, queryFileName);
      throw new RuntimeException(e);
    }
  }
}
