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
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class LocalDataSegmentKillerTest
{
  private static final String DATASOURCE_NAME = "ds";

  private boolean zip;

  public void initLocalDataSegmentKillerTest(boolean zip)
  {
    this.zip = zip;
  }

  public static Iterable<Object[]> constructorFeeder()
  {
    return ImmutableList.of(new Object[]{false}, new Object[]{true});
  }

  @TempDir
  public File temporaryFolder;

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "zip = {0}")
  public void testKill(boolean zip) throws Exception
  {
    initLocalDataSegmentKillerTest(zip);
    LocalDataSegmentKiller killer = new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig());

    // Create following segments and then delete them in this order and assert directory deletions
    // /tmp/dataSource/interval1/v1/0/
    // /tmp/dataSource/interval1/v1/1/
    // /tmp/dataSource/interval1/v2/0/
    // /tmp/dataSource/interval2/v1/0/

    final File dataSourceDir = newFolder(temporaryFolder, DATASOURCE_NAME);

    File interval1Dir = new File(dataSourceDir, "interval1");
    File version11Dir = new File(interval1Dir, "v1");
    File partition011Dir = new File(version11Dir, "0");
    File partition111Dir = new File(version11Dir, "1");

    makePartitionDirWithIndex(partition011Dir);
    makePartitionDirWithIndex(partition111Dir);

    File version21Dir = new File(interval1Dir, "v2");
    File partition021Dir = new File(version21Dir, "0");

    makePartitionDirWithIndex(partition021Dir);

    File interval2Dir = new File(dataSourceDir, "interval2");
    File version12Dir = new File(interval2Dir, "v1");
    File partition012Dir = new File(version12Dir, "0");

    makePartitionDirWithIndex(partition012Dir);

    killer.kill(getSegmentWithPath(partition011Dir));

    Assertions.assertFalse(partition011Dir.exists());
    Assertions.assertTrue(partition111Dir.exists());
    Assertions.assertTrue(partition021Dir.exists());
    Assertions.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(partition111Dir));

    Assertions.assertFalse(version11Dir.exists());
    Assertions.assertTrue(partition021Dir.exists());
    Assertions.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(partition021Dir));

    Assertions.assertFalse(interval1Dir.exists());
    Assertions.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(partition012Dir));

    Assertions.assertFalse(dataSourceDir.exists());
    Assertions.assertTrue(dataSourceDir.getParentFile().exists());
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "zip = {0}")
  public void testKillUniquePath(boolean zip) throws Exception
  {
    initLocalDataSegmentKillerTest(zip);
    final LocalDataSegmentKiller killer = new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig());
    final String uuid = UUID.randomUUID().toString().substring(0, 5);
    final File emptyParentDir = newFolder(temporaryFolder, "junit");
    final File dataSourceDir = new File(emptyParentDir, DATASOURCE_NAME);
    final File intervalDir = new File(dataSourceDir, "interval");
    final File versionDir = new File(intervalDir, "1");
    final File partitionDir = new File(versionDir, "0");
    final File uuidDir = new File(partitionDir, uuid);

    makePartitionDirWithIndex(uuidDir);

    killer.kill(getSegmentWithPath(uuidDir));

    Assertions.assertFalse(uuidDir.exists());
    Assertions.assertFalse(partitionDir.exists());
    Assertions.assertFalse(versionDir.exists());
    Assertions.assertFalse(intervalDir.exists());
    Assertions.assertFalse(dataSourceDir.exists());

    // Verify that we stop after the datasource dir, even though the parent is empty.
    Assertions.assertTrue(emptyParentDir.exists());
    Assertions.assertEquals(0, emptyParentDir.listFiles().length);
  }

  @MethodSource("constructorFeeder")
  @ParameterizedTest(name = "zip = {0}")
  public void testKillUniquePathWrongDataSourceNameInDirectory(boolean zip) throws Exception
  {
    initLocalDataSegmentKillerTest(zip);
    // Verify that
    final LocalDataSegmentKiller killer = new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig());
    final String uuid = UUID.randomUUID().toString().substring(0, 5);
    final File emptyParentDir = newFolder(temporaryFolder, "junit");
    final File dataSourceDir = new File(emptyParentDir, DATASOURCE_NAME + "_wrong");
    final File intervalDir = new File(dataSourceDir, "interval");
    final File versionDir = new File(intervalDir, "1");
    final File partitionDir = new File(versionDir, "0");
    final File uuidDir = new File(partitionDir, uuid);

    makePartitionDirWithIndex(uuidDir);

    killer.kill(getSegmentWithPath(uuidDir));

    Assertions.assertFalse(uuidDir.exists());
    Assertions.assertFalse(partitionDir.exists());
    Assertions.assertFalse(versionDir.exists());
    Assertions.assertFalse(intervalDir.exists());
    Assertions.assertFalse(dataSourceDir.exists());

    // Verify that we stop at 4 pruned paths, even if we don't encounter the datasource-named directory.
    Assertions.assertTrue(emptyParentDir.exists());
    Assertions.assertEquals(0, emptyParentDir.listFiles().length);
  }

  private void makePartitionDirWithIndex(File path) throws IOException
  {
    FileUtils.mkdirp(path);

    if (zip) {
      Assertions.assertTrue(new File(path, LocalDataSegmentPusher.INDEX_ZIP_FILENAME).createNewFile());
    } else {
      Assertions.assertTrue(new File(path, LocalDataSegmentPusher.INDEX_DIR).mkdir());
    }
  }

  private DataSegment getSegmentWithPath(File baseDirectory)
  {
    final String fileName = zip ? LocalDataSegmentPusher.INDEX_ZIP_FILENAME : LocalDataSegmentPusher.INDEX_DIR;
    final File path = new File(baseDirectory, fileName);
    return new DataSegment(
        DATASOURCE_NAME,
        Intervals.of("2000/3000"),
        "ver",
        ImmutableMap.of(
            "type", "local",
            "path", path.toURI().getPath()
        ),
        ImmutableList.of("product"),
        ImmutableList.of("visited_sum", "unique_hosts"),
        NoneShardSpec.instance(),
        9,
        12334
    );
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
