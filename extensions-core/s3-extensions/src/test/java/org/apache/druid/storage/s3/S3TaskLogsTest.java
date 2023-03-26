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
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.base.Optional;
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

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Collectors;

@RunWith(EasyMockRunner.class)
public class S3TaskLogsTest extends EasyMockSupport
{

  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, TEST_PREFIX));
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final long TIME_NOW = 2L;
  private static final long TIME_FUTURE = 3L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new SdkClientException(new IOException());
  private static final Exception NON_RECOVERABLE_EXCEPTION = new SdkClientException(new NullPointerException());
  private static final String LOG_CONTENTS = "log_contents";
  private static final String REPORT_CONTENTS = "report_contents";

  @Mock
  private CurrentTimeMillisSupplier timeSupplier;
  @Mock
  private ServerSideEncryptingAmazonS3 s3Client;

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
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3ObjectSummary objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
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

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.replay(s3Client, timeSupplier);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();
    s3TaskLogs.killAll();

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenDeletingObjects_deletesAllTaskLogs() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1),
        ImmutableMap.of(deleteRequest1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.replay(s3Client, timeSupplier);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();
    s3TaskLogs.killAll();

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
      S3TestUtils.expectListObjects(
          s3Client,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
          .withBucketName(TEST_BUCKET)
          .withKeys(ImmutableList.of(
              new DeleteObjectsRequest.KeyVersion(KEY_1)
          ));
      S3TestUtils.mockS3ClientDeleteObjects(
          s3Client,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(s3Client, timeSupplier);

      S3TaskLogs s3TaskLogs = getS3TaskLogs();
      s3TaskLogs.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_noException_deletesOnlyTaskLogsOlderThan() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3ObjectSummary objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_FUTURE);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1), ImmutableMap.of());

    EasyMock.replay(s3Client, timeSupplier);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();
    s3TaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withBucketName(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1)
        ));

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1),
        ImmutableMap.of(deleteRequest1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.replay(s3Client, timeSupplier);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();
    s3TaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_nonrecoverableExceptionWhenListingObjects_doesntDeleteAnyTaskLogs()
  {
    boolean ioExceptionThrown = false;
    try {
      S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      S3TestUtils.expectListObjects(
          s3Client,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
          .withBucketName(TEST_BUCKET)
          .withKeys(ImmutableList.of(
              new DeleteObjectsRequest.KeyVersion(KEY_1)
          ));
      S3TestUtils.mockS3ClientDeleteObjects(
          s3Client,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.replay(s3Client, timeSupplier);

      S3TaskLogs s3TaskLogs = getS3TaskLogs();
      s3TaskLogs.killOlderThan(TIME_NOW);
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_taskLog_fetch() throws IOException
  {
    EasyMock.reset(s3Client);
    String logPath = TEST_PREFIX + "/" + KEY_1 + "/log";
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(LOG_CONTENTS.length());
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(objectMetadata);

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(LOG_CONTENTS.getBytes(StandardCharsets.UTF_8)));
    GetObjectRequest getObjectRequest = new GetObjectRequest(TEST_BUCKET, logPath);
    getObjectRequest.setRange(0, LOG_CONTENTS.length() - 1);
    getObjectRequest.withMatchingETagConstraint(objectMetadata.getETag());
    EasyMock.expect(s3Client.getObject(getObjectRequest)).andReturn(s3Object);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskLog(KEY_1, 0);
    String taskLogs = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(LOG_CONTENTS, taskLogs);
  }

  @Test
  public void test_taskLog_fetch_withRange() throws IOException
  {
    EasyMock.reset(s3Client);
    String logPath = TEST_PREFIX + "/" + KEY_1 + "/log";
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(LOG_CONTENTS.length());
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(objectMetadata);

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(LOG_CONTENTS.substring(1).getBytes(StandardCharsets.UTF_8)));
    GetObjectRequest getObjectRequest = new GetObjectRequest(TEST_BUCKET, logPath);
    getObjectRequest.setRange(1, LOG_CONTENTS.length() - 1);
    getObjectRequest.withMatchingETagConstraint(objectMetadata.getETag());
    EasyMock.expect(s3Client.getObject(getObjectRequest)).andReturn(s3Object);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskLog(KEY_1, 1);
    String taskLogs = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(LOG_CONTENTS.substring(1), taskLogs);
  }

  @Test
  public void test_taskLog_fetch_withNegativeRange() throws IOException
  {
    EasyMock.reset(s3Client);
    String logPath = TEST_PREFIX + "/" + KEY_1 + "/log";
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(LOG_CONTENTS.length());
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(objectMetadata);

    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(LOG_CONTENTS.substring(1).getBytes(StandardCharsets.UTF_8)));
    GetObjectRequest getObjectRequest = new GetObjectRequest(TEST_BUCKET, logPath);
    getObjectRequest.setRange(1, LOG_CONTENTS.length() - 1);
    getObjectRequest.withMatchingETagConstraint(objectMetadata.getETag());
    EasyMock.expect(s3Client.getObject(getObjectRequest)).andReturn(s3Object);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskLog(KEY_1, -1 * (LOG_CONTENTS.length() - 1));
    String taskLogs = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(LOG_CONTENTS.substring(1), taskLogs);
  }


  @Test
  public void test_report_fetch() throws IOException
  {
    EasyMock.reset(s3Client);
    String logPath = TEST_PREFIX + "/" + KEY_1 + "/report.json";
    ObjectMetadata objectMetadata = new ObjectMetadata();
    objectMetadata.setContentLength(REPORT_CONTENTS.length());
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(objectMetadata);
    S3Object s3Object = new S3Object();
    s3Object.setObjectContent(new ByteArrayInputStream(REPORT_CONTENTS.getBytes(StandardCharsets.UTF_8)));
    GetObjectRequest getObjectRequest = new GetObjectRequest(TEST_BUCKET, logPath);
    getObjectRequest.setRange(0, REPORT_CONTENTS.length() - 1);
    getObjectRequest.withMatchingETagConstraint(objectMetadata.getETag());
    EasyMock.expect(s3Client.getObject(getObjectRequest)).andReturn(s3Object);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskReports(KEY_1);
    String report = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(REPORT_CONTENTS, report);
  }


  @Nonnull
  private S3TaskLogs getS3TaskLogs()
  {
    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix(TEST_PREFIX);
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    inputDataConfig.setMaxListingLength(MAX_KEYS);
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);
    return s3TaskLogs;
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
    CurrentTimeMillisSupplier timeSupplier = new CurrentTimeMillisSupplier();
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    File logFile = tempFolder.newFile("test_log_file");

    s3TaskLogs.pushTaskLog(taskId, logFile);

    return aclExpected.getGrantsAsList();
  }

  @Test
  public void testEnsureQuotated()
  {
    Assert.assertEquals("\"etag\"", S3TaskLogs.ensureQuotated("etag"));
    Assert.assertNull(S3TaskLogs.ensureQuotated(null));
    Assert.assertEquals("\"etag", S3TaskLogs.ensureQuotated("\"etag"));
    Assert.assertEquals("etag\"", S3TaskLogs.ensureQuotated("etag\""));
  }

  @Test
  public void testMatchingEtagConstraintWithEnsureQuotated()
  {
    String eTag = "etag";
    final GetObjectRequest request = new GetObjectRequest(null, null)
        .withMatchingETagConstraint(S3TaskLogs.ensureQuotated(eTag))
        .withRange(0, 1);
    Assert.assertEquals("\"" + eTag + "\"", request.getMatchingETagConstraints().get(0));
  }
}
