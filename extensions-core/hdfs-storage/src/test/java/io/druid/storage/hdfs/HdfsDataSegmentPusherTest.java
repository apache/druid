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

package io.druid.storage.hdfs;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

/**
 */
public class HdfsDataSegmentPusherTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testPushWithScheme() throws Exception
  {
    testUsingScheme("file");
  }

  @Test
  public void testPushWithBadScheme() throws Exception
  {
    expectedException.expect(IOException.class);
    expectedException.expectMessage("No FileSystem for scheme: xyzzy");
    testUsingScheme("xyzzy");

    // Not reached
    Assert.assertTrue(false);
  }

  @Test
  public void testPushWithoutScheme() throws Exception
  {
    testUsingScheme(null);
  }

  private void testUsingScheme(final String scheme) throws Exception
  {
    Configuration conf = new Configuration(true);

    // Create a mock segment on disk
    File segmentDir = tempFolder.newFolder();
    File tmp = new File(segmentDir, "version.bin");

    final byte[] data = new byte[]{0x0, 0x0, 0x0, 0x1};
    Files.write(data, tmp);
    final long size = data.length;

    HdfsDataSegmentPusherConfig config = new HdfsDataSegmentPusherConfig();
    final File storageDirectory = tempFolder.newFolder();

    config.setStorageDirectory(
        scheme != null
        ? String.format("%s://%s", scheme, storageDirectory.getAbsolutePath())
        : storageDirectory.getAbsolutePath()
    );
    HdfsDataSegmentPusher pusher = new HdfsDataSegmentPusher(config, conf, new DefaultObjectMapper());

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

    DataSegment segment = pusher.push(segmentDir, segmentToPush);

    Assert.assertEquals(segmentToPush.getSize(), segment.getSize());
    Assert.assertEquals(segmentToPush, segment);
    Assert.assertEquals(ImmutableMap.of(
        "type",
        "hdfs",
        "path",
        String.format(
            "%s/%s/index.zip",
            config.getStorageDirectory(),
            DataSegmentPusherUtil.getHdfsStorageDir(segmentToPush)
        )
    ), segment.getLoadSpec());
    // rename directory after push
    final String segmentPath = DataSegmentPusherUtil.getHdfsStorageDir(segment);
    File indexFile = new File(String.format("%s/%s/index.zip", storageDirectory, segmentPath));
    Assert.assertTrue(indexFile.exists());
    File descriptorFile = new File(String.format("%s/%s/descriptor.json", storageDirectory, segmentPath));
    Assert.assertTrue(descriptorFile.exists());

    // push twice will fail and temp dir cleaned
    File outDir = new File(String.format("%s/%s", config.getStorageDirectory(), segmentPath));
    outDir.setReadOnly();
    try {
      pusher.push(segmentDir, segmentToPush);
    }
    catch (IOException e) {
      Assert.fail("should not throw exception");
    }
  }
}
