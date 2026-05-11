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
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
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

  private void testPushInternal(boolean useUniquePath, String matcher) throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);

    final AccessControlList acl = new AccessControlList();
    acl.setOwner(new Owner("ownerId", "owner"));
    acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
    EasyMock.expect(s3Client.getBucketAcl(EasyMock.eq("bucket"))).andReturn(acl).once();

    s3Client.upload(EasyMock.anyObject(PutObjectRequest.class));
    EasyMock.expectLastCall().once();

    EasyMock.replay(s3Client);

    validate(useUniquePath, matcher, s3Client);
  }

  private void testPushInternalForEntityTooLarge(boolean useUniquePath, String matcher) throws Exception
  {
    ServerSideEncryptingAmazonS3 s3Client = EasyMock.createStrictMock(ServerSideEncryptingAmazonS3.class);
    final AmazonS3Exception e = new AmazonS3Exception("whoa too many bytes");
    e.setErrorCode(S3Utils.ERROR_ENTITY_TOO_LARGE);


    final AccessControlList acl = new AccessControlList();
    acl.setOwner(new Owner("ownerId", "owner"));
    acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
    EasyMock.expect(s3Client.getBucketAcl(EasyMock.eq("bucket"))).andReturn(acl).once();

    s3Client.upload(EasyMock.anyObject(PutObjectRequest.class));
    EasyMock.expectLastCall().andThrow(e).once();

    EasyMock.replay(s3Client);

    validate(useUniquePath, matcher, s3Client);
  }

  private void validate(boolean useUniquePath, String matcher, ServerSideEncryptingAmazonS3 s3Client) throws IOException
  {
    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");

    S3DataSegmentPusher pusher = new S3DataSegmentPusher(s3Client, config);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

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
    Assert.assertEquals(1, (int) segment.getBinaryVersion());
    Assert.assertEquals("bucket", segment.getLoadSpec().get("bucket"));
    Assert.assertTrue(
        segment.getLoadSpec().get("key").toString(),
        Pattern.compile(matcher).matcher(segment.getLoadSpec().get("key").toString()).matches()
    );
    Assert.assertEquals("s3_zip", segment.getLoadSpec().get("type"));

    EasyMock.verify(s3Client);
  }
}
