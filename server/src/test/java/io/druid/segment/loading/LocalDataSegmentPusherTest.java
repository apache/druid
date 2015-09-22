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

package io.druid.segment.loading;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class LocalDataSegmentPusherTest
{
  DataSegment dataSegment;
  LocalDataSegmentPusher localDataSegmentPusher;
  File dataSegmentFiles;
  File outDir;

  @Before
  public void setUp() throws IOException
  {
    dataSegment = new DataSegment(
        "",
        new Interval(0, 1),
        "",
        null,
        null,
        null,
        new NoneShardSpec(),
        null,
        -1
    );
    localDataSegmentPusher = new LocalDataSegmentPusher(new LocalDataSegmentPusherConfig(), new ObjectMapper());
    dataSegmentFiles = Files.createTempDir();
    ByteStreams.write(
        Ints.toByteArray(0x9),
        Files.newOutputStreamSupplier(new File(dataSegmentFiles, "version.bin"))
    );
  }


  @Test
  public void testPush() throws IOException
  {
    /* DataSegment segment - Used to create LoadSpec and Create outDir (Local Deep Storage location in this case)
       File dataSegmentFile - Used to get location of segment files like version.bin, meta.smoosh and xxxxx.smoosh
      */
    DataSegment returnSegment = localDataSegmentPusher.push(dataSegmentFiles, dataSegment);
    Assert.assertNotNull(returnSegment);
    Assert.assertEquals(dataSegment, returnSegment);
    outDir = new File(
        new LocalDataSegmentPusherConfig().getStorageDirectory(),
        DataSegmentPusherUtil.getStorageDir(returnSegment)
    );
    File versionFile = new File(outDir, "index.zip");
    File descriptorJson = new File(outDir, "descriptor.json");
    Assert.assertTrue(versionFile.exists());
    Assert.assertTrue(descriptorJson.exists());
  }

  @Test
  public void testPathForHadoopAbsolute()
  {
    LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
    config.storageDirectory = new File("/druid");

    Assert.assertEquals(
        "file:/druid/foo",
        new LocalDataSegmentPusher(config, new ObjectMapper()).getPathForHadoop("foo")
    );
  }

  @Test
  public void testPathForHadoopRelative()
  {
    LocalDataSegmentPusherConfig config = new LocalDataSegmentPusherConfig();
    config.storageDirectory = new File("druid");

    Assert.assertEquals(
        String.format("file:%s/druid/foo", System.getProperty("user.dir")),
        new LocalDataSegmentPusher(config, new ObjectMapper()).getPathForHadoop("foo")
    );
  }

  @After
  public void tearDown() throws IOException
  {
    FileUtils.deleteDirectory(dataSegmentFiles);

    if (outDir != null) {
      FileUtils.deleteDirectory(outDir);
    }
  }
}
