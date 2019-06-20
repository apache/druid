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

package org.apache.druid.storage.s3;

import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

public class S3TaskLogsTest
{

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testTaskLogsPushWithAclDisabled() throws Exception
  {
    String ownerId = "test_owner";
    String ownerDisplayName = "test_owner";

    List<Grant> grantList = testPushInternal(true, ownerId, ownerDisplayName);

    Assert.assertTrue("Grant list should not be null", grantList != null);
    Assert.assertEquals("Grant list should be empty as ACL is disabled", 0, grantList.size());
  }

  @Test
  public void testTaskLogsPushWithAclEnabled() throws Exception
  {
    String ownerId = "test_owner";
    String ownerDisplayName = "test_owner";

    List<Grant> grantList = testPushInternal(false, ownerId, ownerDisplayName);

    Assert.assertTrue("Grant list should not be null", grantList != null);
    Assert.assertEquals("Grant list size should be equal to 1", 1, grantList.size());
    Grant grant = grantList.get(0);
    Assert.assertEquals("The Grantee identifier should be test_owner", "test_owner", grant.getGrantee().getIdentifier());
    Assert.assertEquals("The Grant should have full control permission", Permission.FullControl, grant.getPermission());
  }

  private List<Grant> testPushInternal(boolean disableAcl, String ownerId, String ownerDisplayName) throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createMock(ServerSideEncryptingAmazonS3.class);

    EasyMock.expect(s3Client.putObject(EasyMock.anyObject()))
      .andReturn(new PutObjectResult())
      .once();

    AccessControlList aclExpected = new AccessControlList();
    aclExpected.setOwner(new Owner(ownerId, ownerDisplayName));

    EasyMock.expect(s3Client.getBucketAcl("test_bucket"))
      .andReturn(aclExpected)
      .once();

    EasyMock.expect(s3Client.putObject(EasyMock.anyObject(PutObjectRequest.class)))
      .andReturn(new PutObjectResult())
      .once();

    EasyMock.replay(s3Client);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setDisableAcl(disableAcl);
    config.setS3Bucket("test_bucket");
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    File logFile = tempFolder.newFile("test_log_file");

    s3TaskLogs.pushTaskLog(taskId, logFile);

    List<Grant> grantsAsList = aclExpected.getGrantsAsList();

    return grantsAsList;
  }

}
