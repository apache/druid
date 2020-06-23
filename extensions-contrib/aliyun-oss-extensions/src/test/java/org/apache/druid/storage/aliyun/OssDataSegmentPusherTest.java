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

package org.apache.druid.storage.aliyun;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.PutObjectResult;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Pattern;

/**
 *
 */
public class OssDataSegmentPusherTest
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
    testPushInternal(
        true,
        "key/foo/2015-01-01T00:00:00\\.000Z_2016-01-01T00:00:00\\.000Z/0/0/[A-Za-z0-9-]{36}/index\\.zip"
    );
  }

  private void testPushInternal(boolean useUniquePath, String matcher) throws Exception
  {
    OSS client = EasyMock.createStrictMock(OSS.class);

    EasyMock.expect(client.putObject(EasyMock.anyObject()))
            .andReturn(new PutObjectResult())
            .once();

    EasyMock.replay(client);

    OssStorageConfig config = new OssStorageConfig();
    config.setBucket("bucket");
    config.setPrefix("key");

    OssDataSegmentPusher pusher = new OssDataSegmentPusher(client, config);

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
    Assert.assertEquals("oss_zip", segment.getLoadSpec().get("type"));

    EasyMock.verify(client);
  }
}
