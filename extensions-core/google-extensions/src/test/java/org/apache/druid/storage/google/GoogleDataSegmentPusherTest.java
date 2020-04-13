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

package org.apache.druid.storage.google;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.easymock.EasyMock;
import org.easymock.EasyMockSupport;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class GoogleDataSegmentPusherTest extends EasyMockSupport
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  private static final String BUCKET = "bucket";
  private static final String PREFIX = "prefix";

  private GoogleStorage storage;
  private GoogleAccountConfig googleAccountConfig;

  @Before
  public void before()
  {
    storage = createMock(GoogleStorage.class);
    googleAccountConfig = new GoogleAccountConfig();
    googleAccountConfig.setBucket(BUCKET);
    googleAccountConfig.setPrefix(PREFIX);
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
        new HashMap<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        NoneShardSpec.instance(),
        0,
        size
    );

    GoogleDataSegmentPusher pusher = createMockBuilder(GoogleDataSegmentPusher.class)
        .withConstructor(storage, googleAccountConfig)
        .addMockedMethod("insert", File.class, String.class, String.class)
        .createMock();

    final String storageDir = pusher.getStorageDir(segmentToPush, false);
    final String indexPath = PREFIX + "/" + storageDir + "/" + "index.zip";

    pusher.insert(
        EasyMock.anyObject(File.class),
        EasyMock.eq("application/zip"),
        EasyMock.eq(indexPath)
    );
    EasyMock.expectLastCall();

    replayAll();

    DataSegment segment = pusher.push(tempFolder.getRoot(), segmentToPush, false);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals(segmentToPush, segment);
    Assert.assertEquals(ImmutableMap.of(
        "type", GoogleStorageDruidModule.SCHEME,
        "bucket", BUCKET,
        "path", indexPath
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
    GoogleDataSegmentPusher pusher = new GoogleDataSegmentPusher(storage, config);
    Assert.assertEquals("/path", pusher.buildPath("/path"));
    config.setPrefix(null);
    Assert.assertEquals("/path", pusher.buildPath("/path"));
  }

}
