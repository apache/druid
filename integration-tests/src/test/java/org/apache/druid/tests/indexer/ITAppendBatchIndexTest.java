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
import org.apache.druid.indexer.partitions.SingleDimensionPartitionsSpec;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

@Test(groups = {TestNGGroup.APPEND_INGESTION})
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITAppendBatchIndexTest extends AbstractITBatchIndexTest
{
  private static final Logger LOG = new Logger(ITAppendBatchIndexTest.class);
  private static final String INDEX_TASK = "/indexer/wikipedia_local_input_source_index_task.json";
  // This query file is for the initial ingestion which is one complete dataset with roll up
  private static final String INDEX_QUERIES_INITIAL_INGESTION_RESOURCE = "/indexer/wikipedia_index_queries.json";
  // This query file is for the initial ingestion plus the append ingestion which are two complete dataset with roll
  // up within each dataset (roll up within the initial ingestion and roll up within the append ingestion but not
  // roll up across both dataset).
  private static final String INDEX_QUERIES_POST_APPEND_PRE_COMPACT_RESOURCE = "/indexer/wikipedia_double_ingestion_non_perfect_rollup_index_queries.json";
  // This query file is for the initial ingestion plus the append ingestion plus a compaction task after the two ingestions.
  // This is two complete dataset with perfect roll up across both dataset.
  private static final String INDEX_QUERIES_POST_APPEND_POST_COMPACT_RESOURCE = "/indexer/wikipedia_double_ingestion_perfect_rollup_index_queries.json";

  private static final String COMPACTION_TASK = "/indexer/wikipedia_compaction_task.json";

  @DataProvider
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
      compactData(indexDatasource, COMPACTION_TASK);
      // Verification post compaction
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
}
