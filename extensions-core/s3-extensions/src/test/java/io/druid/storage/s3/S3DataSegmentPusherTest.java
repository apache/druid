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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.IOUtils;
import org.easymock.Capture;
import org.easymock.EasyMock;
import org.easymock.IAnswer;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

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
    RestS3Service s3Client = EasyMock.createStrictMock(RestS3Service.class);

    Capture<S3Object> capturedS3Object = Capture.newInstance();
    ValueContainer<String> capturedS3SegmentJson = new ValueContainer<>();
    EasyMock.expect(s3Client.putObject(EasyMock.anyString(), EasyMock.capture(capturedS3Object)))
            .andAnswer(
                new IAnswer<S3Object>()
                {
                  @Override
                  public S3Object answer() throws Throwable
                  {
                    capturedS3SegmentJson.setValue(
                        IOUtils.toString(capturedS3Object.getValue().getDataInputStream(), "utf-8")
                    );
                    return null;
                  }
                }
            )
            .atLeastOnce();
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
        new Interval("2015/2016"),
        "0",
        Maps.<String, Object>newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        NoneShardSpec.instance(),
        0,
        size
    );

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals(1, (int) segment.getBinaryVersion());
    Assert.assertEquals("bucket", segment.getLoadSpec().get("bucket"));
    Assert.assertEquals(
        "key/foo/2015-01-01T00:00:00.000Z_2016-01-01T00:00:00.000Z/0/0/index.zip",
        segment.getLoadSpec().get("key"));
    Assert.assertEquals("s3_zip", segment.getLoadSpec().get("type"));

    // Verify that the pushed S3Object contains the correct data
    String segmentJson = objectMapper.writeValueAsString(segment);
    Assert.assertEquals(segmentJson, capturedS3SegmentJson.getValue());

    EasyMock.verify(s3Client);
  }
}
