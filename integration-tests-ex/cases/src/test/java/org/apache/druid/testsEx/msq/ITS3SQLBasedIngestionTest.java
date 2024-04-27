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
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.S3DeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.indexer.AbstractS3InputSourceParallelIndexTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment
 * DRUID_CLOUD_BUCKET -    s3 Bucket to store in (value to be set in druid.storage.bucket)
 * DRUID_CLOUD_PATH -      path inside the bucket where the test data files will be uploaded
 *                         (this will also be used as druid.storage.baseKey for s3 deep storage setup)
 * <p>
 * The AWS key, secret and region should be set in
 * AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION respectively.
 * <p>
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/s3.html">S3 Deep Storage setup in druid</a>
 */

@RunWith(DruidTestRunner.class)
@Category(S3DeepStorage.class)
public class ITS3SQLBasedIngestionTest extends AbstractS3InputSourceParallelIndexTest
{
  private static final String CLOUD_INGEST_SQL = "/multi-stage-query/wikipedia_cloud_index_msq.sql";
  private static final String INDEX_QUERIES_FILE = "/multi-stage-query/wikipedia_index_queries.json";

  @Test
  @Parameters(method = "resources")
  @TestCaseName("Test_{index} ({0})")
  public void testSQLBasedBatchIngestion(Pair<String, List<?>> s3InputSource)
  {
    doMSQTest(s3InputSource, CLOUD_INGEST_SQL, INDEX_QUERIES_FILE, "s3");
  }
}
