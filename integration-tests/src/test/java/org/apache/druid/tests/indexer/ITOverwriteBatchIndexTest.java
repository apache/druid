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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = TestNGGroup.BATCH_INDEX)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITOverwriteBatchIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  private static final String INDEX_QUERIES_ALL_INGESTION_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INDEX_QUERIES_WITH_DROP_INGESTION_RESOURCE = "/indexer/wikipedia_index_queries_only_data3.json";
  private static final InputFormatDetails INPUT_FORMAT_DETAILS = InputFormatDetails.JSON;
  private static final String ALL_DATA = "*" + INPUT_FORMAT_DETAILS.getFileExtension();
  private static final String ONE_DAY_DATA = "wikipedia_index_data3" + INPUT_FORMAT_DETAILS.getFileExtension();

  @Test
  public void doIndexTestWithOverwriteAndDrop() throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      // Submit initial ingestion task
      // The data interval is 2013-08-31 to 2013-09-02 with DAY segmentGranularity
      // dropExisting true or false does not matter as there is no existing segments
      submitIngestionTaskAndVerify(indexDatasource, ALL_DATA, true);
      verifySegmentsCountAndLoaded(indexDatasource, 4);
      doTestQuery(indexDatasource, INDEX_QUERIES_ALL_INGESTION_RESOURCE);
      // Submit overwrite ingestion task with drop existing
      // The ingestion task interval is the same as the first ingestion ("2013-08-31/2013-09-02"),
      // however, the input data only contains one day of data, 2013-09-02 (instead of 2 days of data)
      // with dropExisting flag set to true, after the second ingestion, we should expect to only have data of 2013-09-02
      submitIngestionTaskAndVerify(indexDatasource, ONE_DAY_DATA, true);
      verifySegmentsCountAndLoaded(indexDatasource, 1);
      doTestQuery(indexDatasource, INDEX_QUERIES_WITH_DROP_INGESTION_RESOURCE);
    }
  }

  @Test
  public void doIndexTestWithOverwriteAndNoDrop() throws Exception
  {
    final String indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      // Submit initial ingestion task
      // The data interval is 2013-08-31 to 2013-09-02 with DAY segmentGranularity
      // dropExisting true or false does not matter as there is no existing segments
      submitIngestionTaskAndVerify(indexDatasource, ALL_DATA, false);
      verifySegmentsCountAndLoaded(indexDatasource, 4);
      doTestQuery(indexDatasource, INDEX_QUERIES_ALL_INGESTION_RESOURCE);
      // Submit overwrite ingestion task without drop existing
      // The ingestion task interval is the same as the first ingestion ("2013-08-31/2013-09-02"),
      // however, the input data only contains one day of data, 2013-09-02 (instead of 2 days of data)
      // with dropExisting flag set to false, after the second ingestion, we should expect to have
      // data from 2013-08-31/2013-09-01 remains unchanged and data for 2013-09-01/2013-09-02 from
      // the second overwrite ingestion task
      submitIngestionTaskAndVerify(indexDatasource, ONE_DAY_DATA, false);
      verifySegmentsCountAndLoaded(indexDatasource, 3);
      doTestQuery(indexDatasource, INDEX_QUERIES_ALL_INGESTION_RESOURCE);
    }
  }

  private void submitIngestionTaskAndVerify(
      String indexDatasource,
      String fileFilter,
      boolean dropExisting
  ) throws Exception
  {
    Map inputFormatMap = new ImmutableMap.Builder<String, Object>().put("type", INPUT_FORMAT_DETAILS.getInputFormatType())
                                                                   .build();
    final Function<String, String> sqlInputSourcePropsTransform = spec -> {
      try {
        spec = StringUtils.replace(
            spec,
            "%%PARTITIONS_SPEC%%",
            jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_FILTER%%",
            fileFilter
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_SOURCE_BASE_DIR%%",
            "/resources/data/batch_index" + INPUT_FORMAT_DETAILS.getFolderSuffix()
        );
        spec = StringUtils.replace(
            spec,
            "%%INPUT_FORMAT%%",
            jsonMapper.writeValueAsString(inputFormatMap)
        );
        spec = StringUtils.replace(
            spec,
            "%%APPEND_TO_EXISTING%%",
            jsonMapper.writeValueAsString(false)
        );
        spec = StringUtils.replace(
            spec,
            "%%DROP_EXISTING%%",
            jsonMapper.writeValueAsString(dropExisting)
        );
        spec = StringUtils.replace(
            spec,
            "%%FORCE_GUARANTEED_ROLLUP%%",
            jsonMapper.writeValueAsString(false)
        );
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
        true,
        new Pair<>(false, false)
    );
  }
}
