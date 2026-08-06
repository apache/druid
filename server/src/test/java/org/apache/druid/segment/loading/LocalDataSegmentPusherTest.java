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
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class LocalDataSegmentPusherTest
{
  @TempDir
  public File temporaryFolder;

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

  @BeforeEach
  public void setUp() throws IOException
  {
    config = new LocalDataSegmentPusherConfig();
    config.zip = false;
    config.storageDirectory = newFolder(temporaryFolder, "junit");
    localDataSegmentPusher = new LocalDataSegmentPusher(config);

    configZip = new LocalDataSegmentPusherConfig();
    configZip.zip = true;
    configZip.storageDirectory = newFolder(temporaryFolder, "junit");
    localDataSegmentPusherZip = new LocalDataSegmentPusher(configZip);

    dataSegmentFiles = newFolder(temporaryFolder, "junit");
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

    Assertions.assertNotNull(returnSegment1);
    Assertions.assertEquals(dataSegment, returnSegment1);

    Assertions.assertNotNull(returnSegment2);
    Assertions.assertEquals(dataSegment2, returnSegment2);

    Assertions.assertNotEquals(
        localDataSegmentPusherZip.getStorageDir(dataSegment, false),
        localDataSegmentPusherZip.getStorageDir(dataSegment2, false)
    );

    for (DataSegment returnSegment : ImmutableList.of(returnSegment1, returnSegment2)) {
      File outDir = new File(
          configZip.getStorageDirectory(),
          localDataSegmentPusherZip.getStorageDir(returnSegment, false)
      );
      File versionFile = new File(outDir, "index.zip");
      Assertions.assertTrue(versionFile.exists());
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

    Assertions.assertNotNull(returnSegment1);
    Assertions.assertEquals(dataSegment, returnSegment1);

    Assertions.assertNotNull(returnSegment2);
    Assertions.assertEquals(dataSegment2, returnSegment2);

    Assertions.assertNotEquals(
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
      Assertions.assertEquals(
          outDir.toURI().getPath(),
          returnSegment.getLoadSpec().get("path")
      );

      // Check for version.bin.
      File versionFile = new File(outDir, "version.bin");
      Assertions.assertTrue(versionFile.exists());
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
    Assertions.assertTrue(pattern.matcher(path).matches(), path);
    Assertions.assertTrue(new File(path).exists());
  }

  @Test
  public void testPushZipUseUniquePath() throws IOException
  {
    DataSegment segment = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, true);

    String path = segment.getLoadSpec().get("path").toString();
    Pattern pattern = Pattern.compile(
        ".*/ds/1970-01-01T00:00:00\\.000Z_1970-01-01T00:00:00\\.001Z/v1/0/[A-Za-z0-9-]{36}/index\\.zip"
    );
    Assertions.assertTrue(pattern.matcher(path).matches(), path);
    Assertions.assertTrue(new File(path).exists());
  }

  @Test
  public void testLastPushWinsForConcurrentNoZipPushes() throws IOException
  {
    // Behavioral difference between zip and no-zip pushes when the same segment identifier is pushed twice:
    // Later zip pushes overwrite earlier ones. Later no-zip pushes throw errors. In situations where the same
    // segment may be pushed twice, we expect "useUniquePath" to be set on the pusher.

    File replicatedDataSegmentFiles = newFolder(temporaryFolder, "junit");
    Files.asByteSink(new File(replicatedDataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x8));
    DataSegment returnSegment1 = localDataSegmentPusher.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusher.push(replicatedDataSegmentFiles, dataSegment2, false);

    Assertions.assertEquals(dataSegment.getDimensions(), returnSegment1.getDimensions());
    Assertions.assertEquals(dataSegment2.getDimensions(), returnSegment2.getDimensions());

    final String expectedPath = StringUtils.format(
        "%s/%s",
        config.storageDirectory,
        "ds/1970-01-01T00:00:00.000Z_1970-01-01T00:00:00.001Z/v1/0/index/"
    );

    Assertions.assertEquals(expectedPath, returnSegment1.getLoadSpec().get("path"));
    Assertions.assertEquals(expectedPath, returnSegment2.getLoadSpec().get("path"));

    final File versionFile = new File(expectedPath, "version.bin");
    Assertions.assertEquals(0x8, Ints.fromByteArray(Files.toByteArray(versionFile)));
  }

  @Test
  public void testLastPushWinsForConcurrentZipPushes() throws IOException
  {
    // Behavioral difference between zip and no-zip pushes when the same segment identifier is pushed twice:
    // Later zip pushes overwrite earlier ones. Later no-zip pushes throw errors. In situations where the same
    // segment may be pushed twice, we expect "useUniquePath" to be set on the pusher.

    File replicatedDataSegmentFiles = newFolder(temporaryFolder, "junit");
    Files.asByteSink(new File(replicatedDataSegmentFiles, "version.bin")).write(Ints.toByteArray(0x8));
    DataSegment returnSegment1 = localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, false);
    DataSegment returnSegment2 = localDataSegmentPusherZip.push(replicatedDataSegmentFiles, dataSegment2, false);

    Assertions.assertEquals(dataSegment.getDimensions(), returnSegment1.getDimensions());
    Assertions.assertEquals(dataSegment2.getDimensions(), returnSegment2.getDimensions());

    File unzipDir = new File(configZip.storageDirectory, "unzip");
    FileUtils.mkdirp(unzipDir);
    CompressionUtils.unzip(
        new File(configZip.storageDirectory, "/ds/1970-01-01T00:00:00.000Z_1970-01-01T00:00:00.001Z/v1/0/index.zip"),
        unzipDir
    );

    Assertions.assertEquals(0x8, Ints.fromByteArray(Files.toByteArray(new File(unzipDir, "version.bin"))));
  }

  @Test
  public void testPushCannotCreateDirectory()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> {
      config.storageDirectory = new File(config.storageDirectory, "xxx");
      Assertions.assertTrue(config.storageDirectory.mkdir());
      config.storageDirectory.setWritable(false);
      localDataSegmentPusher.push(dataSegmentFiles, dataSegment, false);
    });
    assertTrue(exception.getMessage().contains("Cannot create directory"));
  }

  @Test
  public void testPushZipCannotCreateDirectory()
  {
    Throwable exception = org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> {
      configZip.storageDirectory = new File(configZip.storageDirectory, "xxx");
      Assertions.assertTrue(configZip.storageDirectory.mkdir());
      configZip.storageDirectory.setWritable(false);
      localDataSegmentPusherZip.push(dataSegmentFiles, dataSegment, false);
    });
    assertTrue(exception.getMessage().contains("Cannot create directory"));
  }

  private static File newFolder(File root, String... subDirs) throws IOException
  {
    if (subDirs.length == 0 || (subDirs.length == 1 && "junit".equals(subDirs[0]))) {
      return java.nio.file.Files.createTempDirectory(root.toPath(), "junit").toFile();
    }
    String subFolder = String.join("/", subDirs);
    File result = new File(root, subFolder);
    if (!result.mkdirs()) {
      throw new IOException("Couldn't create folders " + root);
    }
    return result;
  }
}
