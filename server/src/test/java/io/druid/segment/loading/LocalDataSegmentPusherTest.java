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
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.StringUtils;
import io.druid.segment.TestHelper;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;

public class LocalDataSegmentPusherTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  LocalDataSegmentPusher localDataSegmentPusher;
  LocalDataSegmentPusherConfig config;
  File dataSegmentFiles;
  DataSegment dataSegment = new DataSegment(
      "ds",
      Intervals.utc(0, 1),
      "v1",
      null,
      null,
      null,
      NoneShardSpec.instance(),
      null,
      -1
  );

  @Before
  public void setUp() throws IOException
  {
    config = new LocalDataSegmentPusherConfig();
    config.storageDirectory = temporaryFolder.newFolder();
    localDataSegmentPusher = new LocalDataSegmentPusher(config, TestHelper.makeJsonMapper());
    dataSegmentFiles = temporaryFolder.newFolder();
    Files.asByteSink(new File(dataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x9));
  }

  @Test
  public void testPush() throws IOException
  {
    /* DataSegment - Used to create LoadSpec and Create outDir (Local Deep Storage location in this case)
       File dataSegmentFile - Used to get location of segment files like version.bin, meta.smoosh and xxxxx.smoosh
      */
    final DataSegment dataSegment2 = dataSegment.withVersion("v2");

    DataSegment returnSegment1 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment);
    DataSegment returnSegment2 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment2);

    Assert.assertNotNull(returnSegment1);
    Assert.assertEquals(dataSegment, returnSegment1);

    Assert.assertNotNull(returnSegment2);
    Assert.assertEquals(dataSegment2, returnSegment2);

    Assert.assertNotEquals(
        localDataSegmentPusher.getStorageDir(dataSegment),
        localDataSegmentPusher.getStorageDir(dataSegment2)
    );

    for (DataSegment returnSegment : ImmutableList.of(returnSegment1, returnSegment2)) {
      File outDir = new File(
          config.getStorageDirectory(),
          localDataSegmentPusher.getStorageDir(returnSegment)
      );
      File versionFile = new File(outDir, "index.zip");
      File descriptorJson = new File(outDir, "descriptor.json");
      Assert.assertTrue(versionFile.exists());
      Assert.assertTrue(descriptorJson.exists());
    }
  }

  @Test
  public void testFirstPushWinsForConcurrentPushes() throws IOException
  {
    File replicatedDataSegmentFiles = temporaryFolder.newFolder();
    Files.asByteSink(new File(replicatedDataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x8));
    DataSegment returnSegment1 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment);
    DataSegment returnSegment2 = localDataSegmentPusher.push(replicatedDataSegmentFiles, dataSegment);

    Assert.assertEquals(returnSegment1, returnSegment2);
  }

  @Test
  public void testPushCannotCreateDirectory() throws IOException
  {
    exception.expect(IOException.class);
    exception.expectMessage("Unable to create directory");
    config.storageDirectory = new File(config.storageDirectory, "xxx");
    Assert.assertTrue(config.storageDirectory.mkdir());
    config.storageDirectory.setWritable(false);
    localDataSegmentPusher.push(dataSegmentFiles, dataSegment);
  }

  @Test
  public void testPathForHadoopAbsolute()
  {
    config.storageDirectory = new File("/druid");

    Assert.assertEquals(
        "file:/druid",
        new LocalDataSegmentPusher(config, new ObjectMapper()).getPathForHadoop()
    );
  }

  @Test
  public void testPathForHadoopRelative()
  {
    config.storageDirectory = new File("druid");

    Assert.assertEquals(
        StringUtils.format("file:%s/druid", System.getProperty("user.dir")),
        new LocalDataSegmentPusher(config, new ObjectMapper()).getPathForHadoop()
    );
  }
}
