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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.ClientException;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.AccessControlList;
import com.aliyun.oss.model.DeleteObjectsRequest;
import com.aliyun.oss.model.Grant;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.Owner;
import com.aliyun.oss.model.PutObjectRequest;
import com.aliyun.oss.model.PutObjectResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@RunWith(EasyMockRunner.class)
public class OssTaskLogsTest extends EasyMockSupport
{

  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("oss://%s/%s", TEST_BUCKET, TEST_PREFIX));
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new ClientException(new IOException());
  private static final Exception NON_RECOVERABLE_EXCEPTION = new ClientException(new NullPointerException());

  @Mock
  private CurrentTimeMillisSupplier timeSupplier;
  @Mock
  private OSS ossClient;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testTaskLogsPushWithAclDisabled() throws Exception
  {
    String ownerId = "test_owner";
    String ownerDisplayName = "test_owner";

    List<Grant> grantList = testPushInternal(true, ownerId, ownerDisplayName);

    Assert.assertNotNull("Grant list should not be null", grantList);
    Assert.assertEquals("Grant list should be empty as ACL is disabled", 0, grantList.size());
  }

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    OSSObjectSummary objectSummary2 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    OssTestUtils.expectListObjects(
        ossClient,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest1.setKeys(Collections.singletonList(KEY_1));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest2.setKeys(Collections.singletonList(KEY_2));

    OssTestUtils.mockClientDeleteObjects(
        ossClient,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.replay(ossClient, timeSupplier);

    OssTaskLogsConfig config = new OssTaskLogsConfig();
    config.setBucket(TEST_BUCKET);
    config.setPrefix(TEST_PREFIX);
    OssInputDataConfig inputDataConfig = new OssInputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
    taskLogs.killAll();

    EasyMock.verify(ossClient, timeSupplier);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenDeletingObjects_deletesAllTaskLogs() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    OssTestUtils.expectListObjects(
        ossClient,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest expectedRequest = new DeleteObjectsRequest(TEST_BUCKET);
    expectedRequest.setKeys(Collections.singletonList(KEY_1));
    OssTestUtils.mockClientDeleteObjects(
        ossClient,
        ImmutableList.of(expectedRequest),
        ImmutableMap.of(expectedRequest, RECOVERABLE_EXCEPTION)
    );

    EasyMock.replay(ossClient, timeSupplier);

    OssTaskLogsConfig config = new OssTaskLogsConfig();
    config.setBucket(TEST_BUCKET);
    config.setPrefix(TEST_PREFIX);
    OssInputDataConfig inputDataConfig = new OssInputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
    taskLogs.killAll();

    EasyMock.verify(ossClient, timeSupplier);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
      OssTestUtils.expectListObjects(
          ossClient,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
      deleteRequest1.setKeys(Collections.singletonList(KEY_1));
      OssTestUtils.mockClientDeleteObjects(
          ossClient,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(ossClient, timeSupplier);

      OssTaskLogsConfig config = new OssTaskLogsConfig();
      config.setBucket(TEST_BUCKET);
      config.setPrefix(TEST_PREFIX);
      OssInputDataConfig inputDataConfig = new OssInputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_KEYS);
      OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
      taskLogs.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(ossClient, timeSupplier);
  }

  @Test
  public void test_killOlderThan_noException_deletesOnlyTaskLogsOlderThan() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    OSSObjectSummary objectSummary2 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_2, TIME_FUTURE);

    OssTestUtils.expectListObjects(
        ossClient,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest1.setKeys(Collections.singletonList(KEY_1));

    OssTestUtils.mockClientDeleteObjects(ossClient, ImmutableList.of(deleteRequest1), ImmutableMap.of());

    EasyMock.replay(ossClient, timeSupplier);

    OssTaskLogsConfig config = new OssTaskLogsConfig();
    config.setBucket(TEST_BUCKET);
    config.setPrefix(TEST_PREFIX);
    OssInputDataConfig inputDataConfig = new OssInputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
    taskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(ossClient, timeSupplier);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    OssTestUtils.expectListObjects(
        ossClient,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
    deleteRequest1.setKeys(Collections.singletonList(KEY_1));

    OssTestUtils.mockClientDeleteObjects(
        ossClient,
        ImmutableList.of(deleteRequest1),
        ImmutableMap.of(deleteRequest1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.replay(ossClient, timeSupplier);

    OssTaskLogsConfig config = new OssTaskLogsConfig();
    config.setBucket(TEST_BUCKET);
    config.setPrefix(TEST_PREFIX);
    OssInputDataConfig inputDataConfig = new OssInputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
    taskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(ossClient, timeSupplier);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      OSSObjectSummary objectSummary1 = OssTestUtils.newOSSObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      OssTestUtils.expectListObjects(
          ossClient,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET);
      deleteRequest1.setKeys(Collections.singletonList(KEY_1));
      OssTestUtils.mockClientDeleteObjects(
          ossClient,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(ossClient, timeSupplier);

      OssTaskLogsConfig config = new OssTaskLogsConfig();
      config.setBucket(TEST_BUCKET);
      config.setPrefix(TEST_PREFIX);
      OssInputDataConfig inputDataConfig = new OssInputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_KEYS);
      OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);
      taskLogs.killOlderThan(TIME_NOW);
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(ossClient, timeSupplier);
  }

  private List<Grant> testPushInternal(boolean disableAcl, String ownerId, String ownerDisplayName) throws Exception
  {
    EasyMock.expect(ossClient.putObject(EasyMock.anyObject()))
            .andReturn(new PutObjectResult())
            .once();

    AccessControlList aclExpected = new AccessControlList();
    aclExpected.setOwner(new Owner(ownerId, ownerDisplayName));

    EasyMock.expect(ossClient.getBucketAcl(TEST_BUCKET))
            .andReturn(aclExpected)
            .once();

    EasyMock.expect(ossClient.putObject(EasyMock.anyObject(PutObjectRequest.class)))
            .andReturn(new PutObjectResult())
            .once();

    EasyMock.replay(ossClient);

    OssTaskLogsConfig config = new OssTaskLogsConfig();
    config.setDisableAcl(disableAcl);
    config.setBucket(TEST_BUCKET);
    CurrentTimeMillisSupplier timeSupplier = new CurrentTimeMillisSupplier();
    OssInputDataConfig inputDataConfig = new OssInputDataConfig();
    OssTaskLogs taskLogs = new OssTaskLogs(ossClient, config, inputDataConfig, timeSupplier);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    File logFile = tempFolder.newFile("test_log_file");

    taskLogs.pushTaskLog(taskId, logFile);

    return new ArrayList<>(aclExpected.getGrants());
  }
}
