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

import org.apache.druid.testing.guice.DruidTestModuleFactory;
import org.apache.druid.tests.TestNGGroup;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Set the bucket and path for your data. This can be done by setting -Ddruid.test.config.cloudBucket and
 *    -Ddruid.test.config.cloudPath or setting "cloud_bucket" and "cloud_path" in the config file.
 * 2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your S3 at the location set in step 1.
 * 3) Provide -Doverride.config.path=<PATH_TO_FILE> with s3 credentials/configs set. See
 *    integration-tests/docker/environment-configs/override-examples/s3 for env vars to provide.
 *    Note that druid_s3_accessKey and druid_s3_secretKey should be unset or set to credentials that does not have
 *    access to the role. The credentials that does have access to the role should be set to the env variable
 *    OVERRIDE_S3_ACCESS_KEY and OVERRIDE_S3_SECRET_KEY
 * 4) Set the assume role configs. This can be done by setting
 *    -Ddruid.test.config.s3AssumeRoleWithExternalId or setting "s3_assume_role_with_external_id" in the config file.
 *    -Ddruid.test.config.s3AssumeRoleExternalId or setting "s3_assume_role_external_id" in the config file.
 *    -Ddruid.test.config.s3AssumeRoleWithoutExternalId or setting "s3_assume_role_without_external_id" in the config file.
 *     The credentials provided in OVERRIDE_S3_ACCESS_KEY and OVERRIDE_S3_SECRET_KEY must be able to assume these roles.
 *     These roles must also have access to the bucket and path for your data in #1.
 *     (s3AssumeRoleExternalId is the external id for s3AssumeRoleWithExternalId, while s3AssumeRoleWithoutExternalId
 *     should not have external id set)
 *
 *     NOTE: Tests in this class will be skipped if properties in #4 are not set.
 */
@Test(groups = TestNGGroup.S3_INGESTION)
@Guice(moduleFactory = DruidTestModuleFactory.class)
public class ITS3AssumeRoleWithOverrideCredentialsIndexTest extends AbstractS3AssumeRoleIndexTest
{
  @Override
  public boolean isSetS3OverrideCredentials()
  {
    return true;
  }

  @Test
  public void testS3WithValidAssumeRoleAndExternalIdUsingOverrideCredentialsShouldSucceed() throws Exception
  {
    doTestS3WithValidAssumeRoleAndExternalIdShouldSucceed();
  }

  @Test
  public void testS3WithAssumeRoleAndInvalidExternalIdUsingOverrideCredentialsShouldFail() throws Exception
  {
    doTestS3WithAssumeRoleAndInvalidExternalIdShouldFail();
  }

  @Test
  public void testS3WithValidAssumeRoleWithoutExternalIdUsingOverrideCredentialsShouldSucceed() throws Exception
  {
    doTestS3WithValidAssumeRoleWithoutExternalIdShouldSucceed();
  }
}
