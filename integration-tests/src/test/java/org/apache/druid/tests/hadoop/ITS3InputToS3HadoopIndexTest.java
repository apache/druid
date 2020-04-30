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

package org.apache.druid.tests.hadoop;

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Set the bucket, path, and region for your data.
 *    This can be done by setting -Ddruid.test.config.cloudBucket, -Ddruid.test.config.cloudPath
 *    and -Ddruid.test.config.cloudRegion or setting "cloud_bucket","cloud_path", and "cloud_region" in the config file.
 * 2) Set -Ddruid.s3.accessKey and -Ddruid.s3.secretKey when running the tests to your access/secret keys.
 * 3) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your S3 at the location set in step 1.
 * 4) Provide -Doverride.config.path=<PATH_TO_FILE> with s3 credentials and hdfs deep storage configs set. See
 *    integration-tests/docker/environment-configs/override-examples/hadoop/s3_to_s3 for env vars to provide.
 * 5) Run the test with -Dstart.hadoop.docker=true -Dextra.datasource.name.suffix='' in the mvn command
 */
@Test(groups = TestNGGroup.HADOOP_S3_TO_S3)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITS3InputToS3HadoopIndexTest extends AbstractS3InputHadoopIndexTest
{
  @Test()
  public void testS3IndexData() throws Exception
  {
    doTest();
  }
}
