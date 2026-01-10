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

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.io.IOUtils;
import org.apache.druid.common.utils.CurrentTimeMillisSupplier;
import org.apache.druid.java.util.common.StringUtils;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.GetBucketAclResponse;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Owner;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import javax.annotation.Nonnull;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
  private static final Exception RECOVERABLE_EXCEPTION = SdkClientException.builder().cause(new IOException()).build();
  private static final Exception NON_RECOVERABLE_EXCEPTION = SdkClientException.builder().cause(new NullPointerException()).build();
  private static final String LOG_CONTENTS = "log_contents";
  private static final String REPORT_CONTENTS = "report_contents";
  private static final String STATUS_CONTENTS = "status_contents";

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
        grant.grantee().id()
    );
    Assert.assertEquals("The Grant should have full control permission", Permission.FULL_CONTROL, grant.permission());
  }

  @Test
  public void test_pushTaskStatus() throws IOException, InterruptedException
  {
    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.isNull(Grant.class));
    EasyMock.expectLastCall().once();

    EasyMock.replay(s3Client);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setDisableAcl(true);

    CurrentTimeMillisSupplier timeSupplier = new CurrentTimeMillisSupplier();
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    File logFile = tempFolder.newFile("status.json");

    s3TaskLogs.pushTaskLog(taskId, logFile);

    EasyMock.verify(s3Client);
  }

  @Test
  public void test_pushTaskPayload() throws IOException, InterruptedException
  {
    Capture<String> bucketCapture = Capture.newInstance(CaptureType.FIRST);
    Capture<String> keyCapture = Capture.newInstance(CaptureType.FIRST);
    Capture<File> fileCapture = Capture.newInstance(CaptureType.FIRST);
    s3Client.upload(EasyMock.capture(bucketCapture), EasyMock.capture(keyCapture), EasyMock.capture(fileCapture), EasyMock.isNull(Grant.class));
    EasyMock.expectLastCall().once();

    EasyMock.replay(s3Client);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix("prefix");
    config.setDisableAcl(true);

    CurrentTimeMillisSupplier timeSupplier = new CurrentTimeMillisSupplier();
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);

    File payloadFile = tempFolder.newFile("task.json");
    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    s3TaskLogs.pushTaskPayload(taskId, payloadFile);

    Assert.assertEquals(TEST_BUCKET, bucketCapture.getValue());
    Assert.assertEquals("prefix/" + taskId + "/task.json", keyCapture.getValue());
    Assert.assertEquals(payloadFile, fileCapture.getValue());
    EasyMock.verify(s3Client);
  }

  @Test
  public void test_streamTaskPayload() throws IOException
  {
    String taskPayloadString = "task payload";

    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) taskPayloadString.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.anyObject(), EasyMock.anyObject()))
        .andReturn(headObjectResponse)
        .once();

    InputStream taskPayload = new ByteArrayInputStream(taskPayloadString.getBytes(Charset.defaultCharset()));
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(getObjectResponse, taskPayload);
    Capture<GetObjectRequest.Builder> getObjectRequestCapture = Capture.newInstance(CaptureType.FIRST);
    EasyMock.expect(s3Client.getObject(EasyMock.capture(getObjectRequestCapture)))
        .andReturn(responseInputStream)
        .once();

    EasyMock.replay(s3Client);

    S3TaskLogsConfig config = new S3TaskLogsConfig();
    config.setS3Bucket(TEST_BUCKET);
    config.setS3Prefix("prefix");
    config.setDisableAcl(true);

    CurrentTimeMillisSupplier timeSupplier = new CurrentTimeMillisSupplier();
    S3InputDataConfig inputDataConfig = new S3InputDataConfig();
    S3TaskLogs s3TaskLogs = new S3TaskLogs(s3Client, config, inputDataConfig, timeSupplier);

    String taskId = "index_test-datasource_2019-06-18T13:30:28.887Z";
    Optional<InputStream> payloadResponse = s3TaskLogs.streamTaskPayload(taskId);

    GetObjectRequest getObjectRequest = getObjectRequestCapture.getValue().build();
    Assert.assertEquals(TEST_BUCKET, getObjectRequest.bucket());
    Assert.assertEquals("prefix/" + taskId + "/task.json", getObjectRequest.key());
    Assert.assertTrue(payloadResponse.isPresent());

    Assert.assertEquals(taskPayloadString, IOUtils.toString(payloadResponse.get(), Charset.defaultCharset()));
    EasyMock.verify(s3Client);
  }

  @Test
  public void test_killAll_noException_deletesAllTaskLogs() throws IOException
  {
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3Object objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(d -> d.objects(ImmutableList.of(
            ObjectIdentifier.builder().key(KEY_1).build()
        )))
        .build();
    DeleteObjectsRequest deleteRequest2 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(d -> d.objects(ImmutableList.of(
            ObjectIdentifier.builder().key(KEY_2).build()
        )))
        .build();

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
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(d -> d.objects(ImmutableList.of(
            ObjectIdentifier.builder().key(KEY_1).build()
        )))
        .build();

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
      S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      EasyMock.expect(timeSupplier.getAsLong()).andReturn(TIME_NOW);
      S3TestUtils.expectListObjects(
          s3Client,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
          .bucket(TEST_BUCKET)
          .delete(d -> d.objects(ImmutableList.of(
              ObjectIdentifier.builder().key(KEY_1).build()
          )))
          .build();
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
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3Object objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_FUTURE);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(d -> d.objects(ImmutableList.of(
            ObjectIdentifier.builder().key(KEY_1).build()
        )))
        .build();

    S3TestUtils.mockS3ClientDeleteObjects(s3Client, ImmutableList.of(deleteRequest1), ImmutableMap.of());

    EasyMock.replay(s3Client, timeSupplier);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();
    s3TaskLogs.killOlderThan(TIME_NOW);

    EasyMock.verify(s3Client, timeSupplier);
  }

  @Test
  public void test_killOlderThan_recoverableExceptionWhenListingObjects_deletesAllTaskLogs() throws IOException
  {
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(d -> d.objects(ImmutableList.of(
            ObjectIdentifier.builder().key(KEY_1).build()
        )))
        .build();

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
      S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
      S3TestUtils.expectListObjects(
          s3Client,
          PREFIX_URI,
          ImmutableList.of(objectSummary1)
      );

      DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
          .bucket(TEST_BUCKET)
          .delete(d -> d.objects(ImmutableList.of(
              ObjectIdentifier.builder().key(KEY_1).build()
          )))
          .build();
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
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) LOG_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);

    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(LOG_CONTENTS.getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
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
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) LOG_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);

    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(LOG_CONTENTS.substring(1).getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
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
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) LOG_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);

    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(LOG_CONTENTS.substring(1).getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
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
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) REPORT_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(REPORT_CONTENTS.getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskReports(KEY_1);
    String report = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(REPORT_CONTENTS, report);
  }

  @Test
  public void test_status_fetch() throws IOException
  {
    EasyMock.reset(s3Client);
    String logPath = TEST_PREFIX + "/" + KEY_1 + "/status.json";
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) STATUS_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(STATUS_CONTENTS.getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
    EasyMock.replay(s3Client);

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskStatus(KEY_1);
    String report = new BufferedReader(
        new InputStreamReader(inputStreamOptional.get(), StandardCharsets.UTF_8))
        .lines()
        .collect(Collectors.joining("\n"));

    Assert.assertEquals(STATUS_CONTENTS, report);
  }

  @Test
  public void test_retryStatusFetch_whenExceptionThrown() throws IOException
  {
    EasyMock.reset(s3Client);
    // throw exception on first call
    S3Exception awsError = (S3Exception) S3Exception.builder()
        .message("AWS Error")
        .statusCode(503)
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(EasyMock.anyString(), EasyMock.anyString())).andThrow(awsError);
    EasyMock.expectLastCall().once();

    String logPath = TEST_PREFIX + "/" + KEY_1 + "/status.json";
    HeadObjectResponse headObjectResponse = HeadObjectResponse.builder()
        .contentLength((long) STATUS_CONTENTS.length())
        .build();
    EasyMock.expect(s3Client.getObjectMetadata(TEST_BUCKET, logPath)).andReturn(headObjectResponse);
    GetObjectResponse getObjectResponse = GetObjectResponse.builder().build();
    ResponseInputStream<GetObjectResponse> responseInputStream = new ResponseInputStream<>(
        getObjectResponse,
        new ByteArrayInputStream(STATUS_CONTENTS.getBytes(StandardCharsets.UTF_8))
    );
    EasyMock.expect(s3Client.getObject(EasyMock.anyObject(GetObjectRequest.Builder.class))).andReturn(responseInputStream);
    EasyMock.expectLastCall().once();

    replayAll();

    S3TaskLogs s3TaskLogs = getS3TaskLogs();

    Optional<InputStream> inputStreamOptional = s3TaskLogs.streamTaskStatus(KEY_1);
    String report;
    try (BufferedReader reader = new BufferedReader(new InputStreamReader(
        inputStreamOptional.get(),
        StandardCharsets.UTF_8
    ))) {
      report = reader.lines().collect(Collectors.joining("\n"));
    }

    Assert.assertEquals(STATUS_CONTENTS, report);
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
    List<Grant> capturedGrants = new ArrayList<>();
    Owner owner = Owner.builder()
        .id(ownerId)
        .displayName(ownerDisplayName)
        .build();

    if (disableAcl) {
      // When ACL is disabled, upload is called with null grant
      s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.isNull(Grant.class));
      EasyMock.expectLastCall().once();
    } else {
      // When ACL is enabled, getBucketAcl is called and a grant is created
      GetBucketAclResponse aclResponse = GetBucketAclResponse.builder()
          .owner(owner)
          .build();
      EasyMock.expect(s3Client.getBucketAcl(TEST_BUCKET))
              .andReturn(aclResponse)
              .once();

      Capture<Grant> grantCapture = Capture.newInstance(CaptureType.FIRST);
      s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.capture(grantCapture));
      EasyMock.expectLastCall().andAnswer(() -> {
        capturedGrants.add(grantCapture.getValue());
        return null;
      }).once();
    }

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

    return capturedGrants;
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
    final GetObjectRequest request = GetObjectRequest.builder()
        .bucket("bucket")
        .key("key")
        .ifMatch(S3TaskLogs.ensureQuotated(eTag))
        .range("bytes=0-1")
        .build();
    Assert.assertEquals("\"" + eTag + "\"", request.ifMatch());
  }
}
