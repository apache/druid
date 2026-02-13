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

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.services.s3.model.DeleteObjectsRequest;
import com.amazonaws.services.s3.model.ListObjectsV2Result;
import com.amazonaws.services.s3.model.MultiObjectDeleteException;
import com.amazonaws.services.s3.model.S3ObjectSummary;
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
import org.easymock.Mock;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

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
  private static final Exception RECOVERABLE_EXCEPTION = new SdkClientException(new IOException());
  private static final Exception NON_RECOVERABLE_EXCEPTION = new SdkClientException(new NullPointerException());

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
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1, TIME_0);
    S3ObjectSummary objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2, TIME_1);

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
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_1_PATH);
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    EasyMock.expectLastCall().andVoid().times(2);


    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_1));
  }

  @Test
  public void test_kill_listOfSegments_multiDeleteExceptionIsThrown()
  {
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_2_PATH);
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    MultiObjectDeleteException.DeleteError deleteError = new MultiObjectDeleteException.DeleteError();
    deleteError.setKey(KEY_1_PATH);
    MultiObjectDeleteException multiObjectDeleteException = new MultiObjectDeleteException(
        ImmutableList.of(deleteError),
        ImmutableList.of());
    EasyMock.expectLastCall().andThrow(multiObjectDeleteException).once();

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
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_2_PATH);
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    MultiObjectDeleteException.DeleteError deleteError = new MultiObjectDeleteException.DeleteError();
    deleteError.setKey(KEY_1_PATH);
    MultiObjectDeleteException multiObjectDeleteException = new MultiObjectDeleteException(
        ImmutableList.of(deleteError),
        ImmutableList.of());
    EasyMock.expectLastCall().andThrow(multiObjectDeleteException).once();
    MultiObjectDeleteException.DeleteError deleteError2 = new MultiObjectDeleteException.DeleteError();
    deleteError2.setKey(KEY_2_PATH);
    MultiObjectDeleteException multiObjectDeleteException2 = new MultiObjectDeleteException(
        ImmutableList.of(deleteError2),
        ImmutableList.of());
    EasyMock.expectLastCall().andThrow(multiObjectDeleteException2).once();

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
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_2_PATH);
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    EasyMock.expectLastCall().andThrow(new AmazonServiceException("")).once();

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
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_1_PATH);
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    MultiObjectDeleteException.DeleteError retryableError = new MultiObjectDeleteException.DeleteError();
    retryableError.setCode("RequestLimitExceeded");
    MultiObjectDeleteException.DeleteError nonRetryableError = new MultiObjectDeleteException.DeleteError();
    nonRetryableError.setCode("nonRetryableError");
    EasyMock.expectLastCall()
        .andThrow(new MultiObjectDeleteException(
            ImmutableList.of(retryableError, nonRetryableError),
            ImmutableList.of()
        ))
        .once();
    EasyMock.expectLastCall().andVoid().times(2);


    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(ImmutableList.of(DATA_SEGMENT_1, DATA_SEGMENT_1));
  }

  @Test
  public void test_kill_listOfSegments_unexpectedExceptionIsThrown()
  {
    DeleteObjectsRequest deleteObjectsRequest = new DeleteObjectsRequest(TEST_BUCKET);
    deleteObjectsRequest.withKeys(KEY_1_PATH, KEY_2_PATH);
    // struggled with the idea of making it match on equaling this
    s3Client.deleteObjects(EasyMock.anyObject(DeleteObjectsRequest.class));
    EasyMock.expectLastCall().andThrow(new AbortedException("")).once();

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
    URI segment1Uri = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, KEY_1 + "/"));
    S3ObjectSummary objectSummary1 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1 + "/00000.smoosh", TIME_0);
    S3ObjectSummary objectSummary2 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1 + "/meta.smoosh", TIME_0);

    S3TestUtils.expectListObjects(s3Client, segment1Uri, ImmutableList.of(objectSummary1, objectSummary2));

    // With MAX_KEYS=1, deleteObjectsInPath batches deletes one key at a time, so two deleteObjects calls
    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(new DeleteObjectsRequest.KeyVersion(KEY_1 + "/00000.smoosh")));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(new DeleteObjectsRequest.KeyVersion(KEY_1 + "/meta.smoosh")));

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET).anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX).anyTimes();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS).anyTimes();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(List.of(DATA_SEGMENT_1_NO_ZIP));
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  @Test
  public void test_kill_not_zipped_multi() throws SegmentLoadingException
  {
    URI segment1Uri = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, KEY_1 + "/"));
    S3ObjectSummary objectSummary11 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1 + "/00000.smoosh", TIME_0);
    S3ObjectSummary objectSummary12 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_1 + "/meta.smoosh", TIME_0);

    // One listObjectsV2 call per segment; no second (empty) page since first result has truncated=false
    S3TestUtils.expectListObjects(s3Client, segment1Uri, ImmutableList.of(objectSummary11, objectSummary12));

    URI segment2Uri = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, KEY_2 + "/"));
    S3ObjectSummary objectSummary21 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2 + "/00000.smoosh", TIME_1);
    S3ObjectSummary objectSummary22 = S3TestUtils.newS3ObjectSummary(TEST_BUCKET, KEY_2 + "/meta.smoosh", TIME_1);
    S3TestUtils.expectListObjects(s3Client, segment2Uri, ImmutableList.of(objectSummary21, objectSummary22));


    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1 + "/00000.smoosh")
        ));
    DeleteObjectsRequest deleteRequest1Meta = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_1 + "/meta.smoosh")
        ));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_2 + "/00000.smoosh")
        ));
    DeleteObjectsRequest deleteRequest2Meta = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(ImmutableList.of(
            new DeleteObjectsRequest.KeyVersion(KEY_2 + "/meta.smoosh")
        ));

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1, deleteRequest1Meta, deleteRequest2, deleteRequest2Meta),
        ImmutableMap.of()
    );

    EasyMock.expect(segmentPusherConfig.getBucket()).andReturn(TEST_BUCKET).anyTimes();
    EasyMock.expect(segmentPusherConfig.getBaseKey()).andReturn(TEST_PREFIX).anyTimes();
    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(MAX_KEYS).anyTimes();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);
    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(List.of(DATA_SEGMENT_1_NO_ZIP, DATA_SEGMENT_2_NO_ZIP));
    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }

  /**
   * Verifies that when a segment directory has more than one page of objects (S3 returns truncated list),
   * the killer paginates through all pages and deletes every object from deep storage.
   */
  @Test
  public void test_kill_not_zipped_pagination_deletesAllObjectsFromDeepStorage() throws SegmentLoadingException
  {
    final int firstPageSize = 1000;
    final int secondPageSize = 100;

    URI segment1Uri = URI.create(StringUtils.format("s3://%s/%s", TEST_BUCKET, KEY_1 + "/"));

    // Create two pages of object summaries to simulate pagination
    ImmutableList.Builder<S3ObjectSummary> firstPageBuilder = ImmutableList.builder();
    ImmutableList.Builder<DeleteObjectsRequest.KeyVersion> allKeysBuilder = ImmutableList.builder();

    for (int i = 0; i < firstPageSize; i++) {
      String key = KEY_1 + "/file_" + i;
      firstPageBuilder.add(S3TestUtils.newS3ObjectSummary(TEST_BUCKET, key, TIME_0));
      allKeysBuilder.add(new DeleteObjectsRequest.KeyVersion(key));
    }

    for (int i = 0; i < secondPageSize; i++) {
      String key = KEY_1 + "/file_" + (firstPageSize + i);
      allKeysBuilder.add(new DeleteObjectsRequest.KeyVersion(key));
    }

    // Mock listObjectsV2 to return results with pagination
    ListObjectsV2Result firstPage = new ListObjectsV2Result();
    firstPage.getObjectSummaries().addAll(firstPageBuilder.build());
    firstPage.setTruncated(true);
    firstPage.setNextContinuationToken("token");

    ImmutableList.Builder<S3ObjectSummary> secondPageBuilder = ImmutableList.builder();
    for (int i = 0; i < secondPageSize; i++) {
      String key = KEY_1 + "/file_" + (firstPageSize + i);
      secondPageBuilder.add(S3TestUtils.newS3ObjectSummary(TEST_BUCKET, key, TIME_0));
    }

    ListObjectsV2Result secondPage = new ListObjectsV2Result();
    secondPage.getObjectSummaries().addAll(secondPageBuilder.build());
    secondPage.setTruncated(false);

    EasyMock.expect(s3Client.listObjectsV2(S3TestUtils.matchListObjectsRequest(segment1Uri)))
            .andReturn(firstPage)
            .once();
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject()))
            .andReturn(secondPage)
            .once();

    // Mock the delete operation - S3 batches deletes in chunks of 1000, so expect two delete calls
    ImmutableList<DeleteObjectsRequest.KeyVersion> allKeys = allKeysBuilder.build();
    DeleteObjectsRequest deleteRequest1 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(allKeys.subList(0, 1000));
    DeleteObjectsRequest deleteRequest2 = new DeleteObjectsRequest(TEST_BUCKET)
        .withKeys(allKeys.subList(1000, 1100));

    S3TestUtils.mockS3ClientDeleteObjects(
        s3Client,
        ImmutableList.of(deleteRequest1, deleteRequest2),
        ImmutableMap.of()
    );

    EasyMock.expect(inputDataConfig.getMaxListingLength()).andReturn(S3InputDataConfig.MAX_LISTING_LENGTH_MAX).anyTimes();

    EasyMock.replay(s3Client, segmentPusherConfig, inputDataConfig);

    segmentKiller = new S3DataSegmentKiller(Suppliers.ofInstance(s3Client), segmentPusherConfig, inputDataConfig);
    segmentKiller.kill(DATA_SEGMENT_1_NO_ZIP);

    EasyMock.verify(s3Client, segmentPusherConfig, inputDataConfig);
  }
}
