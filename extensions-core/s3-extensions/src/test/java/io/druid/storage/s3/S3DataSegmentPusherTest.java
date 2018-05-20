/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.storage.s3;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AccessControlList;
import com.amazonaws.services.s3.model.CanonicalGrantee;
import com.amazonaws.services.s3.model.Grant;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.Permission;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.IOUtils;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;

/**
 */
public class S3DataSegmentPusherTest
{
  private static class ValueContainer<T>
  {
    private T value;

    public T getValue()
    {
      return value;
    }

    public void setValue(T value)
    {
      this.value = value;
    }
  }

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testPush() throws Exception
  {
    testPushInternal(false, "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/index\\.zip");
  }

  @Test
  public void testPushUseUniquePath() throws Exception
  {
    testPushInternal(true, "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/[A-Za-z0-9-]{36}/index\\.zip");
  }

  private void testPushInternal(boolean useUniquePath, String matcher) throws Exception
  {
    AmazonS3Client s3Client = EasyMock.createStrictMock(AmazonS3Client.class);

    final AccessControlList acl = new AccessControlList();
    acl.setOwner(new Owner("ownerId", "owner"));
    acl.grantAllPermissions(new Grant(new CanonicalGrantee(acl.getOwner().getId()), Permission.FullControl));
    EasyMock.expect(s3Client.getBucketAcl(EasyMock.eq("bucket"))).andReturn(acl).once();

    EasyMock.expect(s3Client.putObject(EasyMock.anyObject()))
            .andReturn(new PutObjectResult())
            .once();

    EasyMock.expect(s3Client.getBucketAcl(EasyMock.eq("bucket"))).andReturn(acl).once();

    Capture<PutObjectRequest> capturedPutRequest = Capture.newInstance();
    ValueContainer<String> capturedS3SegmentJson = new ValueContainer<>();
    EasyMock.expect(s3Client.putObject(EasyMock.capture(capturedPutRequest)))
            .andAnswer(
                new IAnswer<PutObjectResult>()
                {
                  @Override
                  public PutObjectResult answer() throws Throwable
                  {
                    capturedS3SegmentJson.setValue(
                        IOUtils.toString(new FileInputStream(capturedPutRequest.getValue().getFile()), "utf-8")
                    );
                    return new PutObjectResult();
                  }
                }
            )
            .once();

    EasyMock.replay(s3Client);

    S3DataSegmentPusherConfig config = new S3DataSegmentPusherConfig();
    config.setBucket("bucket");
    config.setBaseKey("key");

    ObjectMapper objectMapper = new DefaultObjectMapper();
    S3DataSegmentPusher pusher = new S3DataSegmentPusher(s3Client, config, objectMapper);

    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
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
        segment.getLoadSpec().get("key").toString().matches(matcher)
    );
    Assert.assertEquals("s3_zip", segment.getLoadSpec().get("type"));

    // Verify that the pushed S3Object contains the correct data
    String segmentJson = objectMapper.writeValueAsString(segment);
    Assert.assertEquals(segmentJson, capturedS3SegmentJson.getValue());

    EasyMock.verify(s3Client);
  }
}
