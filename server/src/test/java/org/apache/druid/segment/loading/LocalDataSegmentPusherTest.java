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

package org.apache.druid.segment.loading;

import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;
import com.google.common.primitives.Ints;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.druid.utils.CompressionUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

public class LocalDataSegmentPusherTest
{
  @Rule
  public final TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Rule
  public ExpectedException exception = ExpectedException.none();

  LocalDataSegmentPusher localDataSegmentPusher;
  LocalDataSegmentPusher localDataSegmentPusherZip;
  LocalDataSegmentPusherConfig config;
  LocalDataSegmentPusherConfig configZip;
  File dataSegmentFiles;
  DataSegment dataSegment = new DataSegment(
      "ds",
      Intervals.utc(0, 1),
      "v1",
      null,
      ImmutableList.of("dim1"),
      null,
      NoneShardSpec.instance(),
      null,
      0
  );
  DataSegment dataSegment2 = new DataSegment(
      "ds",
      Intervals.utc(0, 1),
      "v1",
      null,
      ImmutableList.of("dim2"),
      null,
      NoneShardSpec.instance(),
      null,
      0
  );

  @Before
  public void setUp() throws IOException
  {
    config = new LocalDataSegmentPusherConfig();
    config.zip = false;
    config.storageDirectory = temporaryFolder.newFolder();
    localDataSegmentPusher = new LocalDataSegmentPusher(config);

    configZip = new LocalDataSegmentPusherConfig();
    configZip.zip = true;
    configZip.storageDirectory = temporaryFolder.newFolder();
    localDataSegmentPusherZip = new LocalDataSegmentPusher(configZip);

    dataSegmentFiles = temporaryFolder.newFolder();
    Files.asByteSink(new File(dataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x9));
  }

  @Test
  public void testPushZip() throws IOException
  {
    /* DataSegment - Used to create LoadSpec and Create outDir (Local Deep Storage location in this case)
       File dataSegmentFile - Used to get location of segment files like version.bin, meta.smoosh and xxxxx.smoosh
      */
    final DataSegment dataSegment2 = dataSegment.withVersion("v2");

    DataSegment returnSegment1 = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment2, false);

    Assert.assertNotNull(returnSegment1);
    Assert.assertEquals(dataSegment, returnSegment1);

    Assert.assertNotNull(returnSegment2);
    Assert.assertEquals(dataSegment2, returnSegment2);

    Assert.assertNotEquals(
        localDataSegmentPusherZip.getStorageDir(dataSegment, false),
        localDataSegmentPusherZip.getStorageDir(dataSegment2, false)
    );

    for (DataSegment returnSegment : ImmutableList.of(returnSegment1, returnSegment2)) {
      File outDir = new File(
          configZip.getStorageDirectory(),
          localDataSegmentPusherZip.getStorageDir(returnSegment, false)
      );
      File versionFile = new File(outDir, "index.zip");
      Assert.assertTrue(versionFile.exists());
    }
  }

  @Test
  public void testPushNoZip() throws IOException
  {
    /* DataSegment - Used to create LoadSpec and Create outDir (Local Deep Storage location in this case)
       File dataSegmentFile - Used to get location of segment files like version.bin, meta.smoosh and xxxxx.smoosh
      */
    final DataSegment dataSegment2 = dataSegment.withVersion("v2");

    DataSegment returnSegment1 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment2, false);

    Assert.assertNotNull(returnSegment1);
    Assert.assertEquals(dataSegment, returnSegment1);

    Assert.assertNotNull(returnSegment2);
    Assert.assertEquals(dataSegment2, returnSegment2);

    Assert.assertNotEquals(
        localDataSegmentPusher.getStorageDir(dataSegment, false),
        localDataSegmentPusher.getStorageDir(dataSegment2, false)
    );

    for (DataSegment returnSegment : ImmutableList.of(returnSegment1, returnSegment2)) {
      File outDir = new File(
          new File(
              config.getStorageDirectory(),
              localDataSegmentPusher.getStorageDir(returnSegment, false)
          ),
          "index"
      );

      // Check against loadSpec.
      Assert.assertEquals(
          outDir.toURI().getPath(),
          returnSegment.getLoadSpec().get("path")
      );

      // Check for version.bin.
      File versionFile = new File(outDir, "version.bin");
      Assert.assertTrue(versionFile.exists());
    }
  }

  @Test
  public void testPushNoZipUseUniquePath() throws IOException
  {
    DataSegment segment = localDataSegmentPusher.push(dataSegmentFiles, dataSegment, true);

    String path = segment.getLoadSpec().get("path").toString();
    Pattern pattern = Pattern.compile(
        ".*/ds/1970-01-01T00:00:00\\.000Z_1970-01-01T00:00:00\\.001Z/v1/0/[A-Za-z0-9-]{36}/index/$"
    );
    Assert.assertTrue(path, pattern.matcher(path).matches());
    Assert.assertTrue(new File(path).exists());
  }

  @Test
  public void testPushZipUseUniquePath() throws IOException
  {
    DataSegment segment = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, true);

    String path = segment.getLoadSpec().get("path").toString();
    Pattern pattern = Pattern.compile(
        ".*/ds/1970-01-01T00:00:00\\.000Z_1970-01-01T00:00:00\\.001Z/v1/0/[A-Za-z0-9-]{36}/index\\.zip"
    );
    Assert.assertTrue(path, pattern.matcher(path).matches());
    Assert.assertTrue(new File(path).exists());
  }

  @Test
  public void testLastPushWinsForConcurrentNoZipPushes() throws IOException
  {
    // Behavioral difference between zip and no-zip pushes when the same segment identifier is pushed twice:
    // Later zip pushes overwrite earlier ones. Later no-zip pushes throw errors. In situations where the same
    // segment may be pushed twice, we expect "useUniquePath" to be set on the pusher.

    File replicatedDataSegmentFiles = temporaryFolder.newFolder();
    Files.asByteSink(new File(replicatedDataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x8));
    DataSegment returnSegment1 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusher.push(replicatedDataSegmentFiles, dataSegment2, false);

    Assert.assertEquals(dataSegment.getDimensions(), returnSegment1.getDimensions());
    Assert.assertEquals(dataSegment2.getDimensions(), returnSegment2.getDimensions());

    final String expectedPath = StringUtils.format(
        "%s/%s",
        config.storageDirectory,
        "ds/1970-01-01T00:00:00.000Z_1970-01-01T00:00:00.001Z/v1/0/index/"
    );

    Assert.assertEquals(expectedPath, returnSegment1.getLoadSpec().get("path"));
    Assert.assertEquals(expectedPath, returnSegment2.getLoadSpec().get("path"));

    final File versionFile = new File(expectedPath, "version.bin");
    Assert.assertEquals(0x8, Ints.fromByteArray(Files.toByteArray(versionFile)));
  }

  @Test
  public void testLastPushWinsForConcurrentZipPushes() throws IOException
  {
    // Behavioral difference between zip and no-zip pushes when the same segment identifier is pushed twice:
    // Later zip pushes overwrite earlier ones. Later no-zip pushes throw errors. In situations where the same
    // segment may be pushed twice, we expect "useUniquePath" to be set on the pusher.

    File replicatedDataSegmentFiles = temporaryFolder.newFolder();
    Files.asByteSink(new File(replicatedDataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x8));
    DataSegment returnSegment1 = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusherZip.push(replicatedDataSegmentFiles, dataSegment2, false);

    Assert.assertEquals(dataSegment.getDimensions(), returnSegment1.getDimensions());
    Assert.assertEquals(dataSegment2.getDimensions(), returnSegment2.getDimensions());

    File unzipDir = new File(configZip.storageDirectory, "unzip");
    FileUtils.mkdirp(unzipDir);
    CompressionUtils.unzip(
        new File(configZip.storageDirectory, "/ds/1970-01-01T00:00:00.000Z_1970-01-01T00:00:00.001Z/v1/0/index.zip"),
        unzipDir
    );

    Assert.assertEquals(0x8, Ints.fromByteArray(Files.toByteArray(new File(unzipDir, "version.bin"))));
  }

  @Test
  public void testPushCannotCreateDirectory() throws IOException
  {
    exception.expect(IOException.class);
    exception.expectMessage("Cannot create directory");
    config.storageDirectory = new File(config.storageDirectory, "xxx");
    Assert.assertTrue(config.storageDirectory.mkdir());
    config.storageDirectory.setWritable(false);
    localDataSegmentPusher.push(dataSegmentFiles, dataSegment, false);
  }

  @Test
  public void testPushZipCannotCreateDirectory() throws IOException
  {
    exception.expect(IOException.class);
    exception.expectMessage("Cannot create directory");
    configZip.storageDirectory = new File(configZip.storageDirectory, "xxx");
    Assert.assertTrue(configZip.storageDirectory.mkdir());
    configZip.storageDirectory.setWritable(false);
    localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, false);
  }

  @Test
  public void testPathForHadoopAbsolute()
  {
    configZip.storageDirectory = new File("/druid");

    // If this test fails because the path is returned as "file:/druid/", this can happen
    // when a /druid directory exists on the local filesystem.
    Assert.assertEquals(
        "file:/druid",
        new LocalDataSegmentPusher(configZip).getPathForHadoop()
    );
  }

  @Test
  public void testPathForHadoopRelative()
  {
    configZip.storageDirectory = new File("druid");

    Assert.assertEquals(
        StringUtils.format("file:%s/druid", System.getProperty("user.dir")),
        new LocalDataSegmentPusher(configZip).getPathForHadoop()
    );
  }
}
