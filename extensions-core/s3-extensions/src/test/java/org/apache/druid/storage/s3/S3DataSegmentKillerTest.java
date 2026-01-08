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

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.LogicalOperator;
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.services.s3.model.Delete;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.net.URI;
import java.util.List;

@RunWith(EasyMockRunner.class)
public class S3DataSegmentKillerTest extends EasyMockSupport
{
  private static final String KEY_1 = "key1";
  private static final String KEY_1_PATH = KEY_1 + "/index.zip";
  private static final String KEY_1_DESCRIPTOR_PATH = KEY_1 + "/descriptor.json";
  private static final String KEY_2 = "key2";
  private static final String KEY_2_PATH = KEY_2 + "/index.zip";
  private static final String TEST_BUCKET = "test_bucket";
  private static final String TEST_PREFIX = "test_prefix";
  private static final URI PREFIX_URI = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, TEST_PREFIX));
  private static final long TIME_0 = 0L;
  private static final long TIME_1 = 1L;
  private static final int MAX_KEYS = 1;
  private static final Exception RECOVERABLE_EXCEPTION = SdkClientException.builder().cause(new IOException()).build();
  private static final Exception NON_RECOVERABLE_EXCEPTION = SdkClientException.builder().cause(new NullPointerException()).build();

  private static final DataSegment DATA_SEGMENT_1 = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("bucket", TEST_BUCKET, "key", KEY_1_PATH),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  private static final DataSegment DATA_SEGMENT_2 = new DataSegment(
      "test",
      Intervals.of("2015-04-13/2015-04-14"),
      "1",
      ImmutableMap.of("bucket", TEST_BUCKET, "key", KEY_2_PATH),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  private static final DataSegment DATA_SEGMENT_1_NO_ZIP = new DataSegment(
      "test",
      Intervals.of("2015-04-12/2015-04-13"),
      "1",
      ImmutableMap.of("bucket", TEST_BUCKET, "key", KEY_1 + "/"),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  private static final DataSegment DATA_SEGMENT_2_NO_ZIP = new DataSegment(
      "test",
      Intervals.of("2015-04-13/2015-04-14"),
      "1",
      ImmutableMap.of("bucket", TEST_BUCKET, "key", KEY_2 + "/"),
      null,
      null,
      NoneShardSpec.instance(),
      0,
      1
  );

  @Mock
  private ServerSideEncryptingAmazonS3 s3Client;
  @Mock
  private S3DataSegmentPusherConfig segmentPusherConfig;
  @Mock
  private S3InputDataConfig inputDataConfig;

  private S3DataSegmentKiller segmentKiller;

  @Test
  public void test_killAll_accountConfigWithNullBucketAndBaseKey_throwsISEException() throws IOException
  {
    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(null);
    EasyMock.expectLastCall().atLeastOnce();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(null);
    EasyMock.expectLastCall().anyTimes();

    boolean thrownISEException = false;

    try {

      EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

      segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
      segmentKiller.killAll();
    }
    catch (ISE e) {
      thrownISEException = true;
    }
    Assert.assertTrue(thrownISEException);
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_noException_deletesAllSegments() throws IOException
  {
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3Object objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1, objectSummary2)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(Delete.builder()
            .objects(ObjectIdentifier.builder().key(KEY_1).build())
            .build())
        .build();
    DeleteObjectsRequest deleteRequest2 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(Delete.builder()
            .objects(ObjectIdentifier.builder().key(KEY_2).build())
            .build())
        .build();

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_recoverableExceptionWhenListingObjects_deletesAllSegments() throws IOException
  {
    S3Object objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);

    S3TestUtils.expectListObjects(
        s3Client,
        PREFIX_URI,
        ImmutableList.of(objectSummary1)
    );

    DeleteObjectsRequest deleteRequest1 = DeleteObjectsRequest.builder()
        .bucket(TEST_BUCKET)
        .delete(Delete.builder()
            .objects(ObjectIdentifier.builder().key(KEY_1).build())
            .build())
        .build();

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1),
        ImmutableMap.of(deleteRequest1, RECOVERABLE_EXCEPTION)
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
    EasyMock.expectLastCall().anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
    EasyMock.expectLastCall().anyTimes();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.killAll();
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_killAll_nonrecoverableExceptionWhenListingObjects_deletesAllSegments()
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
          .delete(Delete.builder()
              .objects(ObjectIdentifier.builder().key(KEY_1).build())
              .build())
          .build();

      S3TestUtils.mockS3ClientDeleteObjects(
          s3Client,
          ImmutableList.of(),
          ImmutableMap.of(deleteRequest1, NON_RECOVERABLE_EXCEPTION)
      );


      EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET);
      EasyMock.expectLastCall().anyTimes();
      EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS);
      EasyMock.expectLastCall().anyTimes();

      EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

      segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
      segmentKiller.killAll();
    }
    catch (IOException e) {
      ioExceptionThrown = true;
    }

    Assert.assertTrue(ioExceptionThrown);
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_kill_singleSegment_doesntexist_passes() throws SegmentLoadingException
  {
    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_PATH)).andReturn(false);
    EasyMock.expectLastCall().once();
    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_DESCRIPTOR_PATH)).andReturn(false);
    EasyMock.expectLastCall().once();
    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(DATA_SEGMENT_1);
  }

  @Test
  public void test_kill_singleSegment_exists_passes() throws SegmentLoadingException
  {
    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_PATH)).andReturn(true);
    EasyMock.expectLastCall().once();

    s3Client.deleteObject(TEST_BUCKET, KEY_1_PATH);
    EasyMock.expectLastCall().andVoid();

    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_DESCRIPTOR_PATH)).andReturn(true);
    EasyMock.expectLastCall().once();

    s3Client.deleteObject(TEST_BUCKET, KEY_1_DESCRIPTOR_PATH);
    EasyMock.expectLastCall().andVoid();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(DATA_SEGMENT_1);
  }

  @Test
  public void test_kill_listOfOneSegment() throws SegmentLoadingException
  {
    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_PATH)).andReturn(true);
    EasyMock.expectLastCall().once();

    s3Client.deleteObject(TEST_BUCKET, KEY_1_PATH);
    EasyMock.expectLastCall().andVoid();

    EasyMock.expect(s3Client.doesObjectExist(TEST_BUCKET, KEY_1_DESCRIPTOR_PATH)).andReturn(true);
    EasyMock.expectLastCall().once();

    s3Client.deleteObject(TEST_BUCKET, KEY_1_DESCRIPTOR_PATH);
    EasyMock.expectLastCall().andVoid();


    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1));
  }

  @Test
  public void test_kill_listOfNoSegments() throws SegmentLoadingException
  {
    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of());
    // has an assertion error if there is an unexpected method call on a mock. Do nothing because we expect the kill
    // method to not interact with mocks
  }

  @Test
  public void test_kill_listOfSegments() throws SegmentLoadingException
  {
    // struggled with the idea of making it match on equaling this
    EasyMock.expect(s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class)))
        .andReturn(DeleteObjectsResponse.builder().build())
        .times(2);


    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_1));
  }

  @Test
  public void test_kill_listOfSegments_multiDeleteExceptionIsThrown()
  {
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    // In v2 SDK, multi-object delete errors are returned in the response, not thrown as exceptions
    // For testing purposes, we simulate an S3Exception being thrown
    S3Exception s3Exception = (S3Exception) S3Exception.builder()
        .message("MultiObjectDeleteException")
        .awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
            .errorCode("MultiObjectDeleteException")
            .errorMessage("Failed to delete: " + KEY_1_PATH)
            .build())
        .statusCode(500)
        .build();
    EasyMock.expectLastCall().andThrow(s3Exception).once();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);

    SegmentLoadingException thrown = Assert.assertThrows(
        SegmentLoadingException.class,
        () -> segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_2))
    );
    Assert.assertEquals("Couldn't delete segments from S3. See the task logs for more details.", thrown.getMessage());
  }

  @Test
  public void test_kill_listOfSegments_multiDeleteExceptionIsThrownMultipleTimes()
  {
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    S3Exception s3Exception1 = (S3Exception) S3Exception.builder()
        .message("MultiObjectDeleteException")
        .awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
            .errorCode("MultiObjectDeleteException")
            .errorMessage("Failed to delete: " + KEY_1_PATH)
            .build())
        .statusCode(500)
        .build();
    EasyMock.expectLastCall().andThrow(s3Exception1).once();
    S3Exception s3Exception2 = (S3Exception) S3Exception.builder()
        .message("MultiObjectDeleteException")
        .awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
            .errorCode("MultiObjectDeleteException")
            .errorMessage("Failed to delete: " + KEY_2_PATH)
            .build())
        .statusCode(500)
        .build();
    EasyMock.expectLastCall().andThrow(s3Exception2).once();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    ImmutableList.Builder<DataSegment> builder = ImmutableList.builder();
    // limit is 1000 per chunk, but we attempt to delete 2 objects per key so this will be 1002 keys so it will make 2
    // calls via the s3client to delete all these objects
    for (int ii = 0; ii < 501; ii++) {
      builder.add(DATA_SEGMENT_1);
    }
    SegmentLoadingException thrown = Assert.assertThrows(
        SegmentLoadingException.class,
        () -> segmentKiller.kill(builder.build())
    );

    Assert.assertEquals("Couldn't delete segments from S3. See the task logs for more details.", thrown.getMessage());
  }

  @Test
  public void test_kill_listOfSegments_amazonServiceExceptionExceptionIsThrown()
  {
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    S3Exception s3Exception = (S3Exception) S3Exception.builder()
        .message("Service exception")
        .statusCode(500)
        .build();
    EasyMock.expectLastCall().andThrow(s3Exception).once();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);

    SegmentLoadingException thrown = Assert.assertThrows(
        SegmentLoadingException.class,
        () -> segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_2))
    );
    Assert.assertEquals("Couldn't delete segments from S3. See the task logs for more details.", thrown.getMessage());
  }

  @Test
  public void test_kill_listOfSegments_retryableExceptionThrown() throws SegmentLoadingException
  {
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    // First call throws retryable exception, then succeeds
    S3Exception retryableException = (S3Exception) S3Exception.builder()
        .message("RequestLimitExceeded")
        .awsErrorDetails(software.amazon.awssdk.awscore.exception.AwsErrorDetails.builder()
            .errorCode("RequestLimitExceeded")
            .errorMessage("Request limit exceeded")
            .build())
        .statusCode(503)
        .build();
    EasyMock.expectLastCall()
        .andThrow(retryableException)
        .once();
    EasyMock.expectLastCall().andReturn(DeleteObjectsResponse.builder().build()).times(2);


    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_1));
  }

  @Test
  public void test_kill_listOfSegments_unexpectedExceptionIsThrown()
  {
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    EasyMock.expectLastCall().andThrow(AbortedException.builder().message("Aborted").build()).once();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);

    SegmentLoadingException thrown = Assert.assertThrows(
        SegmentLoadingException.class,
        () -> segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_2))
    );
    Assert.assertEquals("Couldn't delete segments from S3. See the task logs for more details.", thrown.getMessage());
  }

  @Test
  public void test_kill_not_zipped() throws SegmentLoadingException
  {
    S3Object objectSummary = S3Object.builder()
        .key(KEY_1 + "/meta.smoosh")
        .build();
    S3Object objectSummary2 = S3Object.builder()
        .key(KEY_1 + "/00000.smoosh")
        .build();
    ListObjectsV2Response listResponse = ListObjectsV2Response.builder()
        .contents(List.of(objectSummary, objectSummary2))
        .build();

    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class))).andReturn(listResponse).once();
    s3Client.deleteObject(TEST_BUCKET, KEY_1 + "/00000.smoosh");
    EasyMock.expectLastCall().andVoid();
    s3Client.deleteObject(TEST_BUCKET, KEY_1 + "/meta.smoosh");
    EasyMock.expectLastCall().andVoid();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(List.of(DATA_SEGMENT_1_NO_ZIP));
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_kill_not_zipped_multi() throws SegmentLoadingException
  {
    S3Object objectSummary11 = S3Object.builder()
        .key(KEY_1 + "/meta.smoosh")
        .build();
    S3Object objectSummary12 = S3Object.builder()
        .key(KEY_1 + "/00000.smoosh")
        .build();
    ListObjectsV2Response listResponse1 = ListObjectsV2Response.builder()
        .contents(List.of(objectSummary11, objectSummary12))
        .build();

    S3Object objectSummary21 = S3Object.builder()
        .key(KEY_2 + "/meta.smoosh")
        .build();
    S3Object objectSummary22 = S3Object.builder()
        .key(KEY_2 + "/00000.smoosh")
        .build();
    ListObjectsV2Response listResponse2 = ListObjectsV2Response.builder()
        .contents(List.of(objectSummary21, objectSummary22))
        .build();

    EasyMock.expect(
        s3Client.listObjectsV2(
            EasyMock.cmp(
                ListObjectsV2Request.builder()
                    .bucket(TEST_BUCKET)
                    .prefix(KEY_1 + "/")
                    .build(),
                (o1, o2) -> {
                  if (!o1.bucket().equals(o2.bucket())) {
                    return o1.bucket().compareTo(o2.bucket());
                  }
                  return o1.prefix().compareTo(o2.prefix());
                },
                LogicalOperator.EQUAL
            )
        )
    ).andReturn(listResponse1).once();
    EasyMock.expect(
        s3Client.listObjectsV2(
            EasyMock.cmp(
                ListObjectsV2Request.builder()
                    .bucket(TEST_BUCKET)
                    .prefix(KEY_2 + "/")
                    .build(),
                (o1, o2) -> {
                  if (!o1.bucket().equals(o2.bucket())) {
                    return o1.bucket().compareTo(o2.bucket());
                  }
                  return o1.prefix().compareTo(o2.prefix());
                },
                LogicalOperator.EQUAL
            )
        )
    ).andReturn(listResponse2).once();

    s3Client.deleteObjects(EasyMock.cmp(
        DeleteObjectsRequest.builder()
            .bucket(TEST_BUCKET)
            .delete(Delete.builder()
                .objects(
                    ObjectIdentifier.builder().key(KEY_1 + "/00000.smoosh").build(),
                    ObjectIdentifier.builder().key(KEY_1 + "/meta.smoosh").build(),
                    ObjectIdentifier.builder().key(KEY_2 + "/00000.smoosh").build(),
                    ObjectIdentifier.builder().key(KEY_2 + "/meta.smoosh").build()
                )
                .build())
            .build(),
        (o1, o2) -> {
          if (!o1.bucket().equals(o2.bucket())) {
            return o1.bucket().compareTo(o2.bucket());
          }

          for (ObjectIdentifier key : o1.delete().objects()) {
            boolean found = false;
            for (ObjectIdentifier key2 : o2.delete().objects()) {
              if (key.key().equals(key2.key())) {
                found = true;
              }
            }
            if (!found) {
              return -1;
            }
          }
          return 0;
        },
        LogicalOperator.EQUAL
    ));
    EasyMock.expectLastCall().andReturn(DeleteObjectsResponse.builder().build()).once();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(List.of(DATA_SEGMENT_1_NO_ZIP, DATA_SEGMENT_2_NO_ZIP));
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }
}
