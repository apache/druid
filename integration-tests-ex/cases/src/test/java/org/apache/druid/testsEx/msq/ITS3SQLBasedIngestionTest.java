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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testsEx.categories.S3DeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import junitparams.Parameters;
import junitparams.naming.TestCaseName;


/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Set the bucket and path for your data. This can be done by setting -Ddruid.test.config.cloudBucket and
 *    -Ddruid.test.config.cloudPath or setting "cloud_bucket" and "cloud_path" in the config file.
 * 2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your S3 at the location set in step 1.
 * 3) Provide -Doverride.config.path=<PATH_TO_FILE> with s3 credentials/configs set. See
 *    integration-tests/docker/environment-configs/override-examples/s3 for env vars to provide.
 */

@RunWith(DruidTestRunner.class)
@Category(S3DeepStorage.class)
public class ITS3SQLBasedIngestionTest extends AbstractITSQLBasedIngestion
{
  @Inject
  @Json
  protected ObjectMapper jsonMapper;
  private static final String CLOUD_INGEST_SQL = "/multi-stage-query/wikipedia_cloud_index_msq.sql";
  private static final String INDEX_QUERIES_FILE = "/multi-stage-query/wikipedia_index_queries.json";
  private static final String INPUT_SOURCE_URIS_KEY = "uris";
  private static final String INPUT_SOURCE_PREFIXES_KEY = "prefixes";
  private static final String INPUT_SOURCE_OBJECTS_KEY = "objects";
  private static final String WIKIPEDIA_DATA_1 = "wikipedia_index_data1.json";
  private static final String WIKIPEDIA_DATA_2 = "wikipedia_index_data2.json";
  private static final String WIKIPEDIA_DATA_3 = "wikipedia_index_data3.json";

  public static Object[][] test_cases()
  {
    return new Object[][]{
        {new Pair<>(INPUT_SOURCE_URIS_KEY,
                    ImmutableList.of(
                        "s3://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_1,
                        "s3://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_2,
                        "s3://%%BUCKET%%/%%PATH%%" + WIKIPEDIA_DATA_3
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PREFIXES_KEY,
                    ImmutableList.of(
                        "s3://%%BUCKET%%/%%PATH%%"
                    )
        )},
        {new Pair<>(INPUT_SOURCE_OBJECTS_KEY,
                    ImmutableList.of(
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_1),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_2),
                        ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + WIKIPEDIA_DATA_3)
                    )
        )}
    };
  }

  @Test
  @Parameters(method = "test_cases")
  @TestCaseName("Test_{index} ({0})")
  public void testSQLBasedBatchIngestion(Pair<String, List> s3InputSource)
  {
    try {

      String datasource = "wikipedia_cloud_index_msq";
      String sqlTask = getStringFromFileAndReplaceDatasource(CLOUD_INGEST_SQL, datasource);
      String inputSourceValue = jsonMapper.writeValueAsString(s3InputSource.rhs);
      Map<String, Object> context = ImmutableMap.of("finalizeAggregations", false,
                                                    "maxNumTasks", 5,
                                                    "groupByEnableMultiValueUnnesting", false);

      sqlTask = StringUtils.replace(
          sqlTask,
          "%%INPUT_SOURCE_PROPERTY_KEY%%",
          s3InputSource.lhs
      );
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%INPUT_SOURCE_PROPERTY_VALUE%%",
          inputSourceValue
      );

      submitTask(sqlTask, datasource, context);
      doTestQuery(INDEX_QUERIES_FILE, datasource);

    }
    catch (Exception e) {
      LOG.error(e, "Error while testing [%s] with s3 input source property key [%s]",
                CLOUD_INGEST_SQL, s3InputSource.lhs);
      throw new RuntimeException(e);
    }
  }
}
