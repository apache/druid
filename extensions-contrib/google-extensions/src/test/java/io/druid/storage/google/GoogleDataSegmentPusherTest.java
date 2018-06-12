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

package io.druid.storage.google;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.easymock.EasyMock.expectLastCall;

public class GoogleDataSegmentPusherTest extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String bucket = "bucket";
  private static final String prefix = "prefix";
  private static final String path = "prefix/test/2015-04-12T00:00:00.000Z_2015-04-13T00:00:00.000Z/1/0/index.zip";

  private GoogleStorage storage;
  private GoogleAccountConfig googleAccountConfig;
  private ObjectMapper jsonMapper;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    googleAccountConfig = new GoogleAccountConfig();
    googleAccountConfig.setBucket(bucket);
    googleAccountConfig.setPrefix(prefix);

    jsonMapper = new DefaultObjectMapper();
  }

  @Test
  public void testPush() throws Exception
  {
    // Create a mock segment on disk
    File tmp = tempFolder.newFile("version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    DataSegment segmentToPush = new DataSegment(
        "foo",
        Intervals.of("2015/2016"),
        "0",
        Maps.newHashMap(),
        Lists.<String>newArrayList(),
        Lists.<String>newArrayList(),
        new NoneShardSpec(),
        0,
        size
    );

    GoogleDataSegmentPusher pusher = createMockBuilder(
        GoogleDataSegmentPusher.class
    ).withConstructor(
        storage,
        googleAccountConfig,
         jsonMapper
    ).addMockedMethod("insert", File.class, String.class, String.class).createMock();

    final String storageDir = pusher.getStorageDir(segmentToPush, false);
    final String indexPath = prefix + "/" + storageDir + "/" + "index.zip";
    final String descriptorPath = prefix + "/" + storageDir + "/" + "descriptor.json";

    pusher.insert(
        EasyMock.anyObject(File.class),
        EasyMock.eq("application/zip"),
        EasyMock.eq(indexPath)
    );
    expectLastCall();
    pusher.insert(
        EasyMock.anyObject(File.class),
        EasyMock.eq("application/json"),
        EasyMock.eq(descriptorPath)
    );
    expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush, false);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals(segmentToPush, segment);
    Assert.assertEquals(ImmutableMap.of(
        "type",
        GoogleStorageDruidModule.SCHEME,
        "bucket",
        bucket,
        "path",
        indexPath
    ), segment.getLoadSpec());

    verifyAll();
  }

  @Test
  public void testBuildPath()
  {
    GoogleAccountConfig config = new GoogleAccountConfig();
    StringBuilder sb = new StringBuilder();
    sb.setLength(0);
    config.setPrefix(sb.toString()); // avoid cached empty string
    GoogleDataSegmentPusher pusher = new GoogleDataSegmentPusher(
        storage,
        config,
        jsonMapper
    );
    Assert.assertEquals("/path", pusher.buildPath("/path"));
    config.setPrefix(null);
    Assert.assertEquals("/path", pusher.buildPath("/path"));
  }

}
