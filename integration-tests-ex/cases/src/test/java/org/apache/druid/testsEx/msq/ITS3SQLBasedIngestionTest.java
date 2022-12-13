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

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import junitparams.Parameters;
import junitparams.naming.TestCaseName;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.testsEx.categories.S3DeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;
import java.util.List;
import java.util.Map;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment
 * DRUID_CLOUD_BUCKET, DRUID_CLOUD_PATH, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION
 */

@RunWith(DruidTestRunner.class)
@Category(S3DeepStorage.class)
public class ITS3SQLBasedIngestionTest extends AbstractITSQLBasedIngestion
{
  @Inject
  @Json
  protected ObjectMapper jsonMapper;
  private static AmazonS3 s3Client = s3Client();
  private static String datasource = "wikipedia_cloud_index_msq";
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
                        "s3://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_1,
                        "s3://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_2,
                        "s3://%%BUCKET%%/%%PATH%%/" + WIKIPEDIA_DATA_3
                    )
        )},
        {new Pair<>(INPUT_SOURCE_PREFIXES_KEY,
                    ImmutableList.of(
                        "s3://%%BUCKET%%/%%PATH%%/"
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

  public static AmazonS3 s3Client()
  {
    AWSCredentials credentials = new BasicAWSCredentials(
        System.getenv("AWS_ACCESS_KEY_ID"),
        System.getenv("AWS_SECRET_ACCESS_KEY")
    );
    return AmazonS3ClientBuilder
        .standard()
        .withCredentials(new AWSStaticCredentialsProvider(credentials))
        .withRegion(System.getenv("AWS_REGION"))
        .build();
  }

  public static String[] fileList()
  {
    return new String[] {
        WIKIPEDIA_DATA_1, WIKIPEDIA_DATA_2, WIKIPEDIA_DATA_3
    };
  }

  @BeforeClass
  public static void uploadDataFilesToS3()
  {
    String localPath = "resources/data/batch_index/json/";
    for (String file : fileList()) {
      s3Client.putObject(
          System.getenv("DRUID_CLOUD_BUCKET"),
          System.getenv("DRUID_CLOUD_PATH") + "/" + file,
          new File(localPath + file)
      );
    }
  }

  @AfterClass
  public static void deleteFilesFromS3()
  {
    // Delete uploaded data files
    DeleteObjectsRequest delObjReq = new DeleteObjectsRequest(System.getenv("DRUID_CLOUD_BUCKET"))
        .withKeys(fileList());
    s3Client.deleteObjects(delObjReq);

    // Delete segments created by druid
    ListObjectsRequest listObjectsRequest = new ListObjectsRequest()
        .withBucketName(System.getenv("DRUID_CLOUD_BUCKET"))
        .withPrefix(System.getenv("DRUID_CLOUD_PATH") + "/" + datasource + "/");

    ObjectListing objectListing = s3Client.listObjects(listObjectsRequest);

    while (true) {
      for (S3ObjectSummary objectSummary : objectListing.getObjectSummaries()) {
        s3Client.deleteObject(System.getenv("DRUID_CLOUD_BUCKET"), objectSummary.getKey());
      }
      if (objectListing.isTruncated()) {
        objectListing = s3Client.listNextBatchOfObjects(objectListing);
      } else {
        break;
      }
    }
  }

  @Test
  @Parameters(method = "test_cases")
  @TestCaseName("Test_{index} ({0})")
  public void testSQLBasedBatchIngestion(Pair<String, List> s3InputSource)
  {
    try {
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

      // Setting the correct object path in the sqlTask.
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%BUCKET%%",
          config.getCloudBucket() // Getting from DRUID_CLOUD_BUCKET env variable
      );
      sqlTask = StringUtils.replace(
          sqlTask,
          "%%PATH%%",
          config.getCloudPath() // Getting from DRUID_CLOUD_PATH env variable
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
