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

import junitparams.Parameters;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.S3DeepStorage;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must set the following env variables in the build environment
 * DRUID_CLOUD_BUCKET -    s3 bucket name (value to be set in druid.storage.bucket)
 * DRUID_CLOUD_PATH -      path inside the bucket where the test data files will be uploaded
 *                         (this will also be used as druid.storage.baseKey for s3 deep storage setup)
 * <p>
 * The AWS key, secret and region should be set in AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_REGION respectively.
 * <p>
 * <a href="https://druid.apache.org/docs/latest/development/extensions-core/s3.html">S3 Deep Storage setup in druid</a>
 */

@RunWith(DruidTestRunner.class)
@Category(S3DeepStorage.class)
public class ITS3ToS3ParallelIndexTest extends AbstractS3InputSourceParallelIndexTest
{
  @Test
  @Parameters(method = "resources")
  public void testS3IndexData(Pair<String, List<?>> s3InputSource) throws Exception
  {
    doTest(s3InputSource, new Pair<>(false, false), "s3");
  }
}
