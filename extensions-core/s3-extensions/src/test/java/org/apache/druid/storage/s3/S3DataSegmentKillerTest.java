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
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Request;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(EasyMockRunner.class)
public class S3DataSegmentKillerTest extends EasyMockSupport
{
  private static final String KEY_1 = "key1";
  private static final String KEY_2 = "key2";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final String CONTINUATION_STRING = "continuationToken";
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = new SdkClientException(new IOException());
  private static final Exception NON_RECOVERABLE_EXCEPTION = new SdkClientException(new NullPointerException());

  @Mock private ServerSideEncryptingAmazonS3 s3Client;
  @Mock private S3DataSegmentPusherConfig segmentPusherConfig;
  @Mock private S3InputDataConfig inputDataConfig;

  private S3DataSegmentKiller segmentKiller;

  @Test
  public void test_killAll_noException_deletesAllSegments() throws IOException
  {
    S3ObjectSummary objectSummary1 = S3TestUtils.mockS3ObjectSummary(TIME_0, KEY_1);
    S3ObjectSummary objectSummary2 = S3TestUtils.mockS3ObjectSummary(TIME_1, KEY_2);


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

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(objectSummary1, objectSummary2, request1, request2, result1, result2, s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenListingObjects_deletesAllSegments() throws IOException
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

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(objectSummary1, request1, result1, s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(objectSummary1, request1, result1, s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_deletesAllSegments()
  {
    boolean ioExceptionThrown = false;
    ListObjectsV2Request request1 = null;
    try {
      request1 = S3TestUtils.mockRequest(TEST_BUCKET, TEST_PREFIX, MAX_KEYS, null);

      s3Client = S3TestUtils.mockS3ClientListObjectsV2(
          ImmutableMap.of(),
          ImmutableMap.of(request1, NON_RECOVERABLE_EXCEPTION)
      );

      EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
      EasyMock.expectLastCall().anyTimes();
      EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.replay(request1, s3Client, segmentPusherConfig, inputDataConfig);

      segmentKiller = new S3DataSegmentKiller(s3Client, segmentPusherConfig, inputDataConfig);
      segmentKiller.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);
    EasyMock.verify(request1, s3Client, segmentPusherConfig, inputDataConfig);
  }



}
