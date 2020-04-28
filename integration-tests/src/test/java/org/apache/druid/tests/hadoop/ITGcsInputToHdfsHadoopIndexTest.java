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
 * 1) Set the bucket and path for your data. This can be done by setting -Ddruid.test.config.cloudBucket and
 *    -Ddruid.test.config.cloudPath or setting "cloud_bucket" and "cloud_path" in the config file.
 * 2. Set -Ddruid.test.config.hadoopGcsCredentialsPath to the location of your Google credentials file as it
 *    exists within the Hadoop cluster that will ingest the data. The credentials file can be placed in the
 *    shared folder used by the integration test containers if running the Docker-based Hadoop container,
 *    in which case this property can be set to /shared/<path_of_your_credentials_file>
 * 3) Provide -Dresource.file.dir.path=<PATH_TO_FOLDER> with folder that contains GOOGLE_APPLICATION_CREDENTIALS file
 * 4) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your GCS at the location set in step 1.
 * 5) Provide -Doverride.config.path=<PATH_TO_FILE> with gcs configs set. See
 *    integration-tests/docker/environment-configs/override-examples/hadoop/gcs_to_hdfs for env vars to provide.
 * 6) Run the test with -Dstart.hadoop.docker=true -Dextra.datasource.name.suffix='' in the mvn command
 */
@Test(groups = TestNGGroup.HADOOP_GCS_TO_HDFS)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITGcsInputToHdfsHadoopIndexTest extends AbstractGcsInputHadoopIndexTest
{
  public void testGcsIndexData() throws Exception
  {
    doTest();
  }
}
