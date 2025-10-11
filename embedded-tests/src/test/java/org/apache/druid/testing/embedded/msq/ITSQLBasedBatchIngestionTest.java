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

package org.apache.druid.testing.embedded.msq;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.embedded.indexer.AbstractITBatchIndexTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class ITSQLBasedBatchIngestionTest extends AbstractITBatchIndexTest
{
  private static final String MSQ_TASKS_DIR = "/multi-stage-query/";

  private static final Logger LOG = new Logger(ITSQLBasedBatchIngestionTest.class);

  public static Stream<Arguments> test_cases()
  {
    return Stream.of(
        Arguments.of("msq_inline.sql", "json_path_index_queries.json"),
        Arguments.of("sparse_column_msq.sql", "sparse_column_msq.json"),
        Arguments.of("wikipedia_http_inputsource_msq.sql", "wikipedia_http_inputsource_queries.json"),
        Arguments.of("wikipedia_index_msq.sql", "wikipedia_index_queries.json"),
        Arguments.of("wikipedia_merge_index_msq.sql", "wikipedia_merge_index_queries.json"),
        Arguments.of("wikipedia_index_task_with_transform.sql", "wikipedia_index_queries_with_transform.json")
    );

  }

  @ParameterizedTest(name = "Test_{index} ({0}, {1})")
  @MethodSource("test_cases")
  public void testSQLBasedBatchIngestion(String sqlFileName, String queryFileName)
  {
    try {
      runMSQTaskandTestQueries(
          MSQ_TASKS_DIR + sqlFileName,
          MSQ_TASKS_DIR + queryFileName,
          dataSource,
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
