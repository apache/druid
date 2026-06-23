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

import com.google.common.io.Files;
import org.apache.druid.error.DruidException;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.internal.matchers.ThrowableMessageMatcher;
import org.junit.rules.TemporaryFolder;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.services.s3.model.Grant;
import software.amazon.awssdk.services.s3.model.Grantee;
import software.amazon.awssdk.services.s3.model.Permission;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.Type;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 *
 */
public class S3DataSegmentPusherTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

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
    final DruidException exception = Assert.assertThrows(
        DruidException.class,
        () ->
        testPushInternalForEntityTooLarge(
                false,
                "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/index\\.zip"
        )
    );

    MatcherAssert.assertThat(
            exception,
            ThrowableMessageMatcher.hasMessage(CoreMatchers.startsWith("Got error[EntityTooLarge] from S3"))
    );
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

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig()
    {
      @Override
      public boolean isZip()
      {
        return false;
      }
    };
    config.setBucket("bucket");
    config.setBaseKey("key");
    DataSegment segment = validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        new byte[]{0x0, 0x0, 0x0, 0x1}
    );
    // V1 (test fixture) → not V10 → rangeable stamped as false (skips legacy HEAD probe).
    Assert.assertEquals(Boolean.FALSE, segment.getLoadSpec().get("rangeable"));
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

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig()
    {
      @Override
      public boolean isZip()
      {
        return false;
      }
    };
    config.setBucket("bucket");
    config.setBaseKey("key");

    // version.bin = [0, 0, 0, 0x0A] → IndexIO.V10_VERSION
    DataSegment segment = validate(
        false,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/",
        s3Client,
        config,
        new byte[]{0x0, 0x0, 0x0, 0x0A}
    );
    Assert.assertEquals(10, (int) segment.getBinaryVersion());
    Assert.assertEquals(Boolean.TRUE, segment.getLoadSpec().get("rangeable"));
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
    Assert.assertFalse(segment.getLoadSpec().containsKey("rangeable"));
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
    DataSegment segment = validate(useUniquePath, matcher, s3Client, config, new byte[]{0x0, 0x0, 0x0, 0x1});
    Assert.assertEquals(1, (int) segment.getBinaryVersion());
    return segment;
  }

  private DataSegment validate(
      boolean useUniquePath,
      String matcher,
      ServerSideEncryptingAmazonS3 s3Client,
      S3DataSegmentPusherConfig config,
      byte[] versionBytes
  ) throws IOException
  {
    S3DataSegmentPusher pusher = new S3DataSegmentPusher(s3Client, config);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

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

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush, useUniquePath);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals("bucket", segment.getLoadSpec().get("bucket"));
    Assert.assertTrue(
            segment.getLoadSpec().get("key").toString(),
            Pattern.compile(matcher).matcher(segment.getLoadSpec().get("key").toString()).matches()
    );
    Assert.assertEquals("s3_zip", segment.getLoadSpec().get("type"));

    EasyMock.verify(s3Client);
    return segment;
  }
}
