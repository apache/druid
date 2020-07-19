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
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = TestNGGroup.INPUT_SOURCE)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITSqlInputSourceTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_parallel_index_using_sqlinputsource_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";

  @DataProvider
  public static Object[][] resources()
  {
    return new Object[][]{
        // Multiple query. No filter
        {ImmutableList.of("SELECT * FROM wikipedia_index_data1", "SELECT * FROM wikipedia_index_data2", "SELECT * FROM wikipedia_index_data3")},
        // Multiple query. Filter on timestamp column
        {ImmutableList.of("SELECT * FROM wikipedia_index_data1 WHERE timestamp BETWEEN '2013-08-31 00:00:00' AND '2013-09-02 00:00:00'",
                          "SELECT * FROM wikipedia_index_data2 WHERE timestamp BETWEEN '2013-08-31 00:00:00' AND '2013-09-02 00:00:00'",
                          "SELECT * FROM wikipedia_index_data3 WHERE timestamp BETWEEN '2013-09-01 00:00:00' AND '2013-09-02 00:00:00'")},
        // Multiple query. Filter on data column
        {ImmutableList.of("SELECT * FROM wikipedia_index_data1 WHERE added > 0",
                          "SELECT * FROM wikipedia_index_data2 WHERE added > 0",
                          "SELECT * FROM wikipedia_index_data3 WHERE added > 0")},
        // Single query. No filter
        {ImmutableList.of("SELECT * FROM wikipedia_index_data_all")},
        // Single query. Filter on timestamp column
        {ImmutableList.of("SELECT * FROM wikipedia_index_data_all WHERE timestamp BETWEEN '2013-08-31 00:00:00' AND '2013-09-02 00:00:00'")},
        // Single query. Filter on data column
        {ImmutableList.of("SELECT * FROM wikipedia_index_data_all WHERE added > 0")},
    };
  }

  @Test(dataProvider = "resources")
  public void testIndexData(List<String> sqlQueries) throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> sqlInputSourcePropsTransform = spec -> {
        try {
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          return StringUtils.replace(
              spec,
              "%%SQL_QUERY%%",
              jsonMapper.writeValueAsString(sqlQueries)
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          sqlInputSourcePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true
      );
    }

  }
}
