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

package org.apache.druid.testsEx.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.indexer.partitions.DynamicPartitionsSpec;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;

/**
 * Common abstract class for most cloud deep storage tests
 */
public abstract class AbstractCloudInputSourceParallelIndexTest extends AbstractITBatchIndexTest
{
  private static final String INDEX_TASK = "/indexer/wikipedia_cloud_index_task.json";
  private static final String INDEX_QUERIES_RESOURCE = "/indexer/wikipedia_index_queries.json";
  private static final String INPUT_SOURCE_URIS_KEY = "uris";
  private static final String INPUT_SOURCE_PREFIXES_KEY = "prefixes";
  private static final String INPUT_SOURCE_OBJECTS_KEY = "objects";
  private static final String WIKIPEDIA_DATA_1 = "wikipedia_index_data1.json";
  private static final String WIKIPEDIA_DATA_2 = "wikipedia_index_data2.json";
  private static final String WIKIPEDIA_DATA_3 = "wikipedia_index_data3.json";
  private static final String GOOGLE = "google";
  private static final String AZURE = "azure";
  private static final String AZURE_V2 = "azureStorage";
  private static final String GOOGLE_PREFIX = "googlePrefix";
  private static final String GOOGLE_BUCKET = "googleBucket";
  private static final String AZURE_CONTAINER = "azureContainer";
  private static final String AZURE_STORAGE_ACCOUNT = "azureAccount";
  private static final Logger LOG = new Logger(AbstractCloudInputSourceParallelIndexTest.class);

  String indexDatasource = "wikipedia_cloud_index_test_";

  public static Object[][] resources()
  {
    return new Object[][]{
        {new Pair<>(INPUT_SOURCE_URIS_KEY,
                    ImmutableList.of(
                        "%%INPUT_SOURCE_TYPE%%://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_1,
                        "%%INPUT_SOURCE_TYPE%%://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_2,
                        "%%INPUT_SOURCE_TYPE%%://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_3
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PREFIXES_KEY,
                    ImmutableList.of(
                        "%%INPUT_SOURCE_TYPE%%://%%BUCKET%%/%%PATH%%/"
                    )
        )},
        {new Pair<>(INPUT_SOURCE_OBJECTS_KEY,
                    ImmutableList.of(
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%/" + WIKIPEDIA_DATA_1),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%/" + WIKIPEDIA_DATA_2),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%/" + WIKIPEDIA_DATA_3)
                    )
        )}
    };
  }

  public static List<String> fileList()
  {
    return Arrays.asList(WIKIPEDIA_DATA_1, WIKIPEDIA_DATA_2, WIKIPEDIA_DATA_3);
  }

  public String setInputSourceInPath(String inputSourceType, String inputSourcePath)
  {
    if (GOOGLE.equals(inputSourceType)) {
      return StringUtils.replace(
          inputSourcePath,
          "%%INPUT_SOURCE_TYPE%%",
          "gs"
      );
    } else {
      return StringUtils.replace(
          inputSourcePath,
          "%%INPUT_SOURCE_TYPE%%",
          inputSourceType
      );
    }
  }

  public String getCloudPath(String inputSourceType)
  {
    if (GOOGLE.equals(inputSourceType)) {
      return config.getProperty(GOOGLE_PREFIX);
    } else if (AZURE_V2.equals(inputSourceType)) {
      return config.getProperty(AZURE_CONTAINER) + "/" + config.getCloudPath();
    } else {
      return config.getCloudPath();
    }
  }

  public String getCloudBucket(String inputSourceType)
  {
    if (GOOGLE.equals(inputSourceType)) {
      return config.getProperty(GOOGLE_BUCKET);
    } else if (AZURE.equals(inputSourceType)) {
      return config.getProperty(AZURE_CONTAINER);
    } else if (AZURE_V2.equals(inputSourceType)) {
      return config.getProperty(AZURE_STORAGE_ACCOUNT);
    } else {
      return config.getCloudBucket();
    }
  }

  /**
   * Runs a sql based ingestion test.
   * @param inputSource         input source for ingestion query. Values defined in resource method above can be used for this.
   * @param segmentAvailabilityConfirmationPair   set lhs in the pair to true if you want to confirm that the task waited longer than 0ms for the task to complete.
   *                                              set rhs to true to verify that the segment is actually available.
   * @param inputSourceType     Input source type (eg : s3, gcs, azure)
   * @return                    The datasource used to test.
   */
  String doTest(
      Pair<String, List<?>> inputSource,
      Pair<Boolean, Boolean> segmentAvailabilityConfirmationPair,
      String inputSourceType
  ) throws Exception
  {
    indexDatasource = indexDatasource + UUID.randomUUID();
    try (
        final Closeable ignored1 = unloader(indexDatasource + config.getExtraDatasourceNameSuffix());
    ) {
      final Function<String, String> azurePropsTransform = spec -> {
        try {
          String inputSourceValue = jsonMapper.writeValueAsString(inputSource.rhs);
          inputSourceValue = setInputSourceInPath(inputSourceType, inputSourceValue);
          inputSourceValue = StringUtils.replace(
              inputSourceValue,
              "%%BUCKET%%",
              getCloudBucket(inputSourceType)
          );
          inputSourceValue = StringUtils.replace(
              inputSourceValue,
              "%%PATH%%",
              getCloudPath(inputSourceType)
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_FORMAT_TYPE%%",
              InputFormatDetails.JSON.getInputFormatType()
          );
          spec = StringUtils.replace(
              spec,
              "%%PARTITIONS_SPEC%%",
              jsonMapper.writeValueAsString(new DynamicPartitionsSpec(null, null))
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_TYPE%%",
              inputSourceType
          );
          spec = StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_KEY%%",
              inputSource.lhs
          );
          return StringUtils.replace(
              spec,
              "%%INPUT_SOURCE_PROPERTY_VALUE%%",
              inputSourceValue
          );
        }
        catch (Exception e) {
          throw new RuntimeException(e);
        }
      };

      doIndexTest(
          indexDatasource,
          INDEX_TASK,
          azurePropsTransform,
          INDEX_QUERIES_RESOURCE,
          false,
          true,
          true,
          segmentAvailabilityConfirmationPair
      );
      return indexDatasource;
    }
  }

  /**
   * Runs a sql based ingestion test.
   * @param inputSource         input source for ingestion query. Values defined in resource method above can be used for this.
   * @param ingestSQLFilePath   file containing the sql query that will be used to ingest the input source data.
   * @param testQueriesFilePath file containing the test queries that need to run to test ingested datasource.
   *                            Should also contain expected results for those queries
   * @param inputSourceType     Input source type (eg : s3, gcs, azure)
   */
  public void doMSQTest(Pair<String, List<?>> inputSource,
                        String ingestSQLFilePath,
                        String testQueriesFilePath,
                        String inputSourceType
  )
  {
    try {
      indexDatasource = "wikipedia_index_test_" + UUID.randomUUID();
      String sqlTask = getStringFromFileAndReplaceDatasource(ingestSQLFilePath, indexDatasource);
      String inputSourceValue = jsonMapper.writeValueAsString(inputSource.rhs);
      Map<String, Object> context = ImmutableMap.of("finalizeAggregations", false,
                                                    "maxNumTasks", 5,
                                                    "groupByEnableMultiValueUnnesting", false);

      sqlTask = StringUtils.replace(
          sqlTask,
          "%%INPUT_SOURCE_TYPE%%",
          inputSourceType
      );
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%INPUT_SOURCE_PROPERTY_KEY%%",
          inputSource.lhs
      );
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%INPUT_SOURCE_PROPERTY_VALUE%%",
          inputSourceValue
      );

      // Setting the correct object path in the sqlTask.
      sqlTask = setInputSourceInPath(inputSourceType, sqlTask);
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%BUCKET%%",
          getCloudBucket(inputSourceType)
      );
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%PATH%%",
          getCloudPath(inputSourceType)
      );

      submitMSQTask(sqlTask, indexDatasource, context);

      // Verifying ingested datasource
      doTestQuery(indexDatasource, testQueriesFilePath);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing [%s] with s3 input source property key [%s]",
                ingestSQLFilePath, inputSource.lhs);
      throw new RuntimeException(e);
    }
  }
}
