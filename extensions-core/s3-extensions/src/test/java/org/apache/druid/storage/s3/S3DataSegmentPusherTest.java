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

import com.google.common.collect.ImmutableSet;
import com.google.common.io.Files;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.DeepStorageSegmentConfig;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.Capture;
import org.easymock.CaptureType;
import org.easymock.EasyMock;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.DeleteObjectsRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectsResponse;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectIdentifier;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 *
 */
public class S3DataSegmentPusherTest
{
  @TempDir
  public File tempFolder;

  @Test
  public void testPush() throws Exception
  {
    testPushInternal(
            false,
            "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/index\\.zip"
    );
  }

  @Test
  public void testPushUseUniquePath() throws Exception
  {
    testPushInternal(
            true,
            "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/[A-Za-z0-9-]{36}/index\\.zip"
    );
  }

  @Test
  public void testEntityTooLarge()
  {
    final DruidException exception = Assertions.assertThrows(
        DruidException.class,
        () ->
        testPushInternalForEntityTooLarge(
                false,
                "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/index\\.zip"
        )
    );

    Assertions.assertTrue(exception.getMessage().startsWith("Got error[EntityTooLarge] from S3"));
  }

  @Test
  public void testPushNoZip() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    // nothing else is under the path, so there is nothing to clean up
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(ListObjectsV2Response.builder().isTruncated(false).build())
            .once();

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");
    DataSegment segment = validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        false,
        new byte[]{0x0, 0x0, 0x0, 0x1}
    );
    // V1 (test fixture) → not V10 → rangeable stamped as false (skips legacy HEAD probe).
    Assertions.assertEquals(Boolean.FALSE, segment.getLoadSpec().get("rangeable"));
  }

  @Test
  public void testPushNoZipV10StampsRangeableTrue() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    // nothing else is under the path, so there is nothing to clean up
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(ListObjectsV2Response.builder().isTruncated(false).build())
            .once();

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");

    // version.bin = [0, 0, 0, 0x0A] → IndexIO.V10_VERSION
    DataSegment segment = validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        false,
        new byte[]{0x0, 0x0, 0x0, 0x0A}
    );
    Assertions.assertEquals(10, (int) segment.getBinaryVersion());
    Assertions.assertEquals(Boolean.TRUE, segment.getLoadSpec().get("rangeable"));
  }

  @Test
  public void testPushNoZipRemovesObjectsLeftByPreviousPush() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    // an index.zip from a zipped push and a smoosh chunk from a larger prior segment: nothing in the new segment
    // references either, but the puller would list and download both
    final String prefix = "key/foo/2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z/0/0/";
    final String staleZip = prefix + "index.zip";
    final String staleChunk = prefix + "00001.smoosh";
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(
                ListObjectsV2Response.builder()
                                     .contents(
                                         S3Object.builder().key(prefix + "version.bin").size(4L).build(),
                                         S3Object.builder().key(staleZip).size(128L).build(),
                                         S3Object.builder().key(staleChunk).size(64L).build()
                                     )
                                     .isTruncated(false)
                                     .build()
            )
            .once();

    final Capture<DeleteObjectsRequest> deleteRequest = Capture.newInstance();
    EasyMock.expect(s3Client.deleteObjects(EasyMock.capture(deleteRequest)))
            .andReturn(DeleteObjectsResponse.builder().build())
            .once();

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");
    validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        false,
        new byte[]{0x0, 0x0, 0x0, 0x1}
    );

    // only the objects this push did not write are removed
    Assertions.assertEquals(
        ImmutableSet.of(staleZip, staleChunk),
        deleteRequest.getValue().delete().objects().stream().map(ObjectIdentifier::key).collect(Collectors.toSet())
    );
  }

  @Test
  public void testPushNoZipBatchesStaleObjectDeletesWithDefaultConfig() throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    // maxListingLength defaults to 1024, which is a legal listing size but more keys than DeleteObjects accepts, so
    // the deletes have to be split even though the listing is not
    final String prefix = "key/foo/2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z/0/0/";
    final int staleCount = S3Utils.MAX_MULTI_OBJECT_DELETE_SIZE + 1;
    final List<S3Object> listing = new ArrayList<>();
    listing.add(S3Object.builder().key(prefix + "version.bin").size(4L).build());
    for (int i = 0; i < staleCount; i++) {
      listing.add(S3Object.builder().key(StringUtils.format("%s%05d.smoosh", prefix, i)).size(8L).build());
    }
    EasyMock.expect(s3Client.listObjectsV2(EasyMock.anyObject(ListObjectsV2Request.class)))
            .andReturn(ListObjectsV2Response.builder().contents(listing).isTruncated(false).build())
            .once();

    final Capture<DeleteObjectsRequest> deleteRequests = Capture.newInstance(CaptureType.ALL);
    EasyMock.expect(s3Client.deleteObjects(EasyMock.capture(deleteRequests)))
            .andReturn(DeleteObjectsResponse.builder().build())
            .times(2);

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");
    Assertions.assertEquals(1024, config.getMaxListingLength());

    validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        false,
        new byte[]{0x0, 0x0, 0x0, 0x1}
    );

    // no request may carry more keys than S3 accepts, and between them they remove every stale object
    for (DeleteObjectsRequest request : deleteRequests.getValues()) {
      Assertions.assertTrue(
          request.delete().objects().size() <= S3Utils.MAX_MULTI_OBJECT_DELETE_SIZE,
          "DeleteObjects request carried " + request.delete().objects().size() + " keys"
      );
    }
    Assertions.assertEquals(
        staleCount,
        deleteRequests.getValues().stream().mapToInt(request -> request.delete().objects().size()).sum()
    );
  }

  @Test
  public void testPushZipDoesNotStampRangeable() throws Exception
  {
    // Zip path uses the no-flag makeLoadSpec overload; openRangeReader returns null on the zip-key short circuit
    // regardless, but we keep the loadSpec JSON compact for zipped segments by omitting the field entirely.
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    EasyMock.replay(s3Client);

    DataSegment segment = validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/index\\.zip",
        s3Client
    );
    Assertions.assertFalse(segment.getLoadSpec().containsKey("rangeable"));
  }

  private void testPushInternal(boolean useUniquePath, String matcher) throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().once();

    EasyMock.replay(s3Client);

    validate(useUniquePath, matcher, s3Client);
  }

  private void testPushInternalForEntityTooLarge(boolean useUniquePath, String matcher) throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final S3Exception e = (S3Exception) S3Exception.builder()
        .message("whoa too many bytes")
        .awsErrorDetails(AwsErrorDetails.builder()
                                        .errorCode(S3Utils.ERROR_ENTITY_TOO_LARGE)
                                        .errorMessage("whoa too many bytes")
                                        .build())
        .statusCode(400)
        .build();

    Grant grant = Grant.builder()
        .grantee(Grantee.builder().id("ownerId").type(Type.CANONICAL_USER).build())
        .permission(Permission.FULL_CONTROL)
        .build();
    EasyMock.expect(s3Client.getBucketOwnerGrant(EasyMock.eq("bucket"))).andReturn(grant).once();

    s3Client.upload(EasyMock.anyString(), EasyMock.anyString(), EasyMock.anyObject(File.class), EasyMock.anyObject(Grant.class));
    EasyMock.expectLastCall().andThrow(e).once();

    EasyMock.replay(s3Client);

    validate(useUniquePath, matcher, s3Client);
  }

  private DataSegment validate(boolean useUniquePath, String matcher, ServerSideEncryptingAmazonS3 s3Client) throws IOException
  {
    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");
    // Default version.bin is V1 for historical reasons.
    DataSegment segment = validate(useUniquePath, matcher, s3Client, config, true, new byte[]{0x0, 0x0, 0x0, 0x1});
    Assertions.assertEquals(1, (int) segment.getBinaryVersion());
    return segment;
  }

  private DataSegment validate(
      boolean useUniquePath,
      String matcher,
      ServerSideEncryptingAmazonS3 s3Client,
      S3DataSegmentPusherConfig config,
      boolean zip,
      byte[] versionBytes
  ) throws IOException
  {
    S3DataSegmentPusher pusher = new S3DataSegmentPusher(s3Client, config, new DeepStorageSegmentConfig(zip));

    // Create a mock segment on disk
    File tmp = new File(tempFolder, "version.bin");

    Files.write(versionBytes, tmp);
    final long size = versionBytes.length;

    DataSegment segmentToPush = new DataSegment(
            "foo",
            Intervals.of("2015/2016"),
            "0",
            new HashMap<>(),
            new ArrayList<>(),
            new ArrayList<>(),
            NoneShardSpec.instance(),
            0,
            size
    );

    DataSegment segment = pusher.push(tempFolder, segmentToPush, useUniquePath);

    Assertions.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assertions.assertEquals("bucket", segment.getLoadSpec().get("bucket"));
    Assertions.assertTrue(
            Pattern.compile(matcher).matcher(segment.getLoadSpec().get("key").toString()).matches(),
            segment.getLoadSpec().get("key").toString()
    );
    Assertions.assertEquals("s3_zip", segment.getLoadSpec().get("type"));

    EasyMock.verify(s3Client);
    return segment;
  }
}
