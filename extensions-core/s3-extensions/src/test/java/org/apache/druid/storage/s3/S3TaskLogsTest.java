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

import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.common.utils.TimeSupplier;
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
import java.util.List;

@RunWith(EasyMockRunner.class)
public class S3TaskLogsTest extends EasyMockSupport
{

  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final String CONTINUATION_STRING = "continuationToken";
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new SdkClientException(new IOException());
  private static final Exception NON_RECOVERABLE_EXCEPTION = new SdkClientException(new NullPointerException());

  @Mock private TimeSupplier timeSupplier;
  @Mock private ServerSideEncryptingAmazonS3 s3Client;

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
  public void testTaskLogsPushWithAclEnabled() throws Exception
  {
    String ownerId = "test_owner";
    String ownerDisplayName = "test_owner";

    List<Grant> grantList = testPushInternal(false, ownerId, ownerDisplayName);

    Assert.assertNotNull("Grant list should not be null", grantList);
    Assert.assertEquals("Grant list size should be equal to 1", 1, grantList.size());
    Grant grant = grantList.get(0);
    Assert.assertEquals(
        "The Grantee identifier should be test_owner",
        "test_owner",
        grant.getGrantee().getIdentifier()
    );
    Assert.assertEquals("The Grant should have full control permission", Permission.FullControl, grant.getPermission());
  }

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.mockS3ObjectSummary(TIME_0, KEY_1);
    S3ObjectSummary objectSummary2 = S3TestUtils.mockS3ObjectSummary(TIME_1, KEY_2);

    EasyMock.expect(timeSupplier.get()).andReturn(TIME_NOW);

    ListObjectsV2Request request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);
    ListObjectsV2Result result1 = S3TestUtils.mockResult(CONTINUATION_STRING, true, ImmutableList.of(objectSummary1));

    ListObjectsV2Request request2 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, CONTINUATION_STRING);
    ListObjectsV2Result result2 = S3TestUtils.mockResult(null, false, ImmutableList.of(objectSummary2));

    s3Client = S3TestUtils.mockS3ClientListObjectsV2(
        ImmutableMap.of(
            request1, result1,
            request2, result2
        ),
        ImmutableMap.of()
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_2)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1, deleteRequest2), ImmutableList.of());

    EasyMock.replay(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, timeSupplier);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix(TEST_PREFIX);
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
    s3TaskLogs.killAll();

    EasyMock.verify(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, timeSupplier);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.mockS3ObjectSummary(TIME_0, KEY_1);

    EasyMock.expect(timeSupplier.get()).andReturn(TIME_NOW);

    ListObjectsV2Request request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);
    ListObjectsV2Result result1 = S3TestUtils.mockResult(null, false, ImmutableList.of(objectSummary1));

    s3Client = S3TestUtils.mockS3ClientListObjectsV2(
        ImmutableMap.of(
            request1, result1
        ),
        ImmutableMap.of(request1, RECOVERABLE_EXCEPTION)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1), ImmutableList.of());

    EasyMock.replay(objectSummary1, request1, result1, s3Client, timeSupplier);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix(TEST_PREFIX);
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
    s3TaskLogs.killAll();

    EasyMock.verify(objectSummary1, request1, result1, s3Client, timeSupplier);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    ListObjectsV2Request request1 = null;
    try {
      EasyMock.expect(timeSupplier.get()).andReturn(TIME_NOW);

      request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);

      s3Client = S3TestUtils.mockS3ClientListObjectsV2(
          ImmutableMap.of(),
          ImmutableMap.of(request1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(request1, s3Client, timeSupplier);

      S3TaskLogsConfig config = new S3TaskLogsConfig();
      config.setS3Bucket(TEST_BUCKET);
      config.setS3Prefix(TEST_PREFIX);
      S3InputDataConfig inputDataConfig = new S3InputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_KEYS);
      S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
      s3TaskLogs.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(request1, s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_noException_deletesOnlyTaskLogsOlderThan() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.mockS3ObjectSummary(TIME_0, KEY_1);
    S3ObjectSummary objectSummary2 = S3TestUtils.mockS3ObjectSummary(TIME_FUTURE, KEY_2);

    ListObjectsV2Request request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);
    ListObjectsV2Result result1 = S3TestUtils.mockResult(CONTINUATION_STRING, true, ImmutableList.of(objectSummary1));

    ListObjectsV2Request request2 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, CONTINUATION_STRING);
    ListObjectsV2Result result2 = S3TestUtils.mockResult(null, false, ImmutableList.of(objectSummary2));

    s3Client = S3TestUtils.mockS3ClientListObjectsV2(
        ImmutableMap.of(
            request1, result1,
            request2, result2
        ),
        ImmutableMap.of()
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_2)
        ));
    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1), ImmutableList.of(deleteRequest2));

    EasyMock.replay(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, timeSupplier);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix(TEST_PREFIX);
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
    s3TaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.mockS3ObjectSummary(TIME_0, KEY_1);

    ListObjectsV2Request request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);
    ListObjectsV2Result result1 = S3TestUtils.mockResult(null, false, ImmutableList.of(objectSummary1));

    s3Client = S3TestUtils.mockS3ClientListObjectsV2(
        ImmutableMap.of(
            request1, result1
        ),
        ImmutableMap.of(request1, RECOVERABLE_EXCEPTION)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1), ImmutableList.of());

    EasyMock.replay(objectSummary1, request1, result1, s3Client, timeSupplier);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix(TEST_PREFIX);
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
    s3TaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(objectSummary1, request1, result1, s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    ListObjectsV2Request request1 = null;
    try {
      request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);

      s3Client = S3TestUtils.mockS3ClientListObjectsV2(
          ImmutableMap.of(),
          ImmutableMap.of(request1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(request1, s3Client, timeSupplier);

      S3TaskLogsConfig config = new S3TaskLogsConfig();
      config.setS3Bucket(TEST_BUCKET);
      config.setS3Prefix(TEST_PREFIX);
      S3InputDataConfig inputDataConfig = new S3InputDataConfig();
      inputDataConfig.setMaxListingLength(MAX_KEYS);
      S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
      s3TaskLogs.killOlderThan(TIME_NOW);
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(request1, s3Client, timeSupplier);
  }

  private List<Grant> testPushInternal(boolean disableAcl, String ownerId, String ownerDisplayName) throws Exception
  {
    EasyMock.expect(s3Client.putObject(EasyMock.anyObject()))
            .andReturn(new PutObjectResult())
            .once();

    AccessControlList aclExpected = new AccessControlList();
    aclExpected.setOwner(new Owner(ownerId, ownerDisplayName));

    EasyMock.expect(s3Client.getBucketAcl(TEST_BUCKET))
            .andReturn(aclExpected)
            .once();

    EasyMock.expect(s3Client.putObject(EasyMock.anyObject(PutObjectRequest.class)))
            .andReturn(new PutObjectResult())
            .once();

    EasyMock.replay(s3Client);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setDisableAcl(disableAcl);
    config.setS3Bucket(TEST_BUCKET);
    TimeSupplier timeSupplier = new TimeSupplier();
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    File logFile = tempFolder.newFile("test_log_file");

    s3TaskLogs.pushTaskLog(taskId, logFile);

    List<Grant> grantsAsList = aclExpected.getGrantsAsList();

    return grantsAsList;
  }
}
