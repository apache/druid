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

package org.apache.druid.storage.hdfs;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.loading.SegmentLoadingException;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NoneShardSpec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HdfsDataSegmentKillerTest
{

  @Test
  public void testKill() throws Exception
  {
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return "/tmp";
          }
        }
    );

    FileSystem fs = FileSystem.get(config);

    // Create following segments and then delete them in this order and assert directory deletions
    // /tmp/dataSource/interval1/v1/0/index.zip
    // /tmp/dataSource/interval1/v1/1/index.zip
    // /tmp/dataSource/interval1/v2/0/index.zip
    // /tmp/dataSource/interval2/v1/0/index.zip

    Path dataSourceDir = new Path("/tmp/dataSource");

    Path interval1Dir = new Path(dataSourceDir, "interval1");
    Path version11Dir = new Path(interval1Dir, "v1");
    Path partition011Dir = new Path(version11Dir, "0");
    Path partition111Dir = new Path(version11Dir, "1");

    makePartitionDirWithIndex(fs, partition011Dir);
    makePartitionDirWithIndex(fs, partition111Dir);

    Path version21Dir = new Path(interval1Dir, "v2");
    Path partition021Dir = new Path(version21Dir, "0");

    makePartitionDirWithIndex(fs, partition021Dir);

    Path interval2Dir = new Path(dataSourceDir, "interval2");
    Path version12Dir = new Path(interval2Dir, "v1");
    Path partition012Dir = new Path(version12Dir, "0");

    makePartitionDirWithIndex(fs, partition012Dir);

    killer.kill(getSegmentWithPath(new Path(partition011Dir, "index.zip").toString()));

    Assertions.assertFalse(fs.exists(partition011Dir));
    Assertions.assertTrue(fs.exists(partition111Dir));
    Assertions.assertTrue(fs.exists(partition021Dir));
    Assertions.assertTrue(fs.exists(partition012Dir));

    killer.kill(getSegmentWithPath(new Path(partition111Dir, "index.zip").toString()));

    Assertions.assertFalse(fs.exists(version11Dir));
    Assertions.assertTrue(fs.exists(partition021Dir));
    Assertions.assertTrue(fs.exists(partition012Dir));

    killer.kill(getSegmentWithPath(new Path(partition021Dir, "index.zip").toString()));

    Assertions.assertFalse(fs.exists(interval1Dir));
    Assertions.assertTrue(fs.exists(partition012Dir));

    killer.kill(getSegmentWithPath(new Path(partition012Dir, "index.zip").toString()));

    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.delete(dataSourceDir, false));
  }

  @Test
  public void testKillForSegmentPathWithoutPartitionNumber() throws Exception
  {
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return "/tmp";
          }
        }
    );

    FileSystem fs = FileSystem.get(config);
    Path dataSourceDir = new Path("/tmp/dataSourceNew");

    Path interval1Dir = new Path(dataSourceDir, "intervalNew");
    Path version11Dir = new Path(interval1Dir, "v1");

    Assertions.assertTrue(fs.mkdirs(version11Dir));
    fs.createNewFile(new Path(version11Dir, StringUtils.format("%s_index.zip", 3)));

    killer.kill(getSegmentWithPath(new Path(version11Dir, "3_index.zip").toString()));

    Assertions.assertFalse(fs.exists(version11Dir));
    Assertions.assertFalse(fs.exists(interval1Dir));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.exists(new Path("/tmp")));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.delete(dataSourceDir, false));
  }

  @Test
  public void testKillForSegmentWithUniquePath() throws Exception
  {
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return "/tmp";
          }
        }
    );

    FileSystem fs = FileSystem.get(config);
    Path dataSourceDir = new Path("/tmp/dataSourceNew");

    Path interval1Dir = new Path(dataSourceDir, "intervalNew");
    Path version11Dir = new Path(interval1Dir, "v1");
    String uuid = UUID.randomUUID().toString().substring(0, 5);

    Assertions.assertTrue(fs.mkdirs(version11Dir));
    fs.createNewFile(new Path(version11Dir, StringUtils.format("%s_%s_index.zip", 3, uuid)));

    killer.kill(getSegmentWithPath(new Path(version11Dir, StringUtils.format("%s_%s_index.zip", 3, uuid)).toString()));

    Assertions.assertFalse(fs.exists(version11Dir));
    Assertions.assertFalse(fs.exists(interval1Dir));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.exists(new Path("/tmp")));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.delete(dataSourceDir, false));
  }

  @Test
  public void testKillLz4Segment() throws Exception
  {
    final File testRoot = FileUtils.createTempDir();
    final Configuration config = new Configuration();
    final HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return testRoot.getAbsolutePath();
          }
        }
    );

    final FileSystem fs = FileSystem.get(config);
    final Path versionDir = new Path(testRoot.getAbsolutePath(), "dataSource/interval/v1");
    final Path segmentPath = new Path(versionDir, "3_index.lz4");
    try {
      Assertions.assertTrue(fs.mkdirs(versionDir));
      fs.createNewFile(segmentPath);

      killer.kill(getSegmentWithPath(segmentPath.toString()));

      Assertions.assertFalse(fs.exists(segmentPath));
      Assertions.assertFalse(fs.exists(versionDir));
      Assertions.assertFalse(fs.exists(versionDir.getParent()));
      Assertions.assertTrue(fs.exists(new Path(testRoot.getAbsolutePath(), "dataSource")));
    }
    finally {
      fs.delete(new Path(testRoot.getAbsolutePath()), true);
    }
  }

  @Test
  public void testKillNonExistingSegment() throws Exception
  {
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return "/tmp";
          }
        }
    );

    // Should do nothing.
    killer.kill(getSegmentWithPath(new Path("/xxx/", "index.zip").toString()));
  }

  @Test
  public void testKillRecursive_forWhenConstructedPathReturnsNull() throws Exception
  {
    final File testRoot = FileUtils.createTempDir();
    final Configuration config = new Configuration();
    final FileSystem fs = FileSystem.get(config);
    try {
      final HdfsDataSegmentKiller killerWithStorage = new HdfsDataSegmentKiller(
          config,
          new HdfsDataSegmentPusherConfig()
          {
            @Override
            public String getStorageDirectory()
            {
              return testRoot.getAbsolutePath();
            }
          }
      );

      final Path workspaceRoot = new Path(testRoot.getAbsolutePath(), "workspace");
      final Path nested = new Path(workspaceRoot, "evil");
      Assertions.assertTrue(fs.mkdirs(nested));
      fs.createNewFile(new Path(nested, "probe"));

      final Path stagingRun = new Path(new Path(testRoot.getAbsolutePath(), "staging"), "some_run_id");
      Assertions.assertTrue(fs.mkdirs(stagingRun));

      killerWithStorage.killRecursively(null);
      killerWithStorage.killRecursively("");
      killerWithStorage.killRecursively("/absolute/under/root");
      killerWithStorage.killRecursively("path\\with\\backslashes");
      killerWithStorage.killRecursively("workspace/../evil");
      killerWithStorage.killRecursively("only/../dots");
      killerWithStorage.killRecursively("..");
      killerWithStorage.killRecursively("workspace//evil");
      killerWithStorage.killRecursively("workspace/evil/");

      Assertions.assertTrue(fs.exists(nested), "workspace/evil should survive null constructHdfsDeletePath cases");
      Assertions.assertTrue(fs.exists(stagingRun));

      final HdfsDataSegmentKiller killerNoStorage = new HdfsDataSegmentKiller(
          config,
          new HdfsDataSegmentPusherConfig()
          {
            @Override
            public String getStorageDirectory()
            {
              return "";
            }
          }
      );
      killerNoStorage.killRecursively("staging/some_run_id");
      Assertions.assertTrue(fs.exists(stagingRun), "paths must not be deleted when storage directory is unset");
    }
    finally {
      fs.delete(new Path(testRoot.getAbsolutePath()), true);
    }
  }

  @Test
  public void testKillRecursively_missingDirectoryIsNoOp() throws Exception
  {
    final File testRoot = FileUtils.createTempDir();
    final Configuration config = new Configuration();
    final HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return testRoot.getAbsolutePath();
          }
        }
    );

    final FileSystem fs = FileSystem.get(config);
    try {
      killer.killRecursively("staging/no_such_directory");
    }
    finally {
      fs.delete(new Path(testRoot.getAbsolutePath()), true);
    }
  }

  @Test
  public void testKillRecursively() throws Exception
  {
    final File testRoot = FileUtils.createTempDir();
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return testRoot.getAbsolutePath();
          }
        }
    );

    final FileSystem fs = FileSystem.get(config);
    try {
      Path parentDir = new Path(testRoot.getAbsolutePath(), "export");
      Path taskDir = new Path(new Path(parentDir, "run_a"), "leaf");
      Assertions.assertTrue(fs.mkdirs(taskDir.getParent()));
      fs.createNewFile(taskDir);

      killer.killRecursively("export/run_a");

      Assertions.assertFalse(fs.exists(new Path(parentDir, "run_a")));
      Assertions.assertTrue(fs.exists(parentDir));
      Assertions.assertTrue(fs.delete(parentDir, true));
    }
    finally {
      fs.delete(new Path(testRoot.getAbsolutePath()), true);
    }
  }

  /**
   * {@link HdfsDataSegmentPusher#pushToPath} replaces {@code ':'} with {@code '_'} in
   * storage suffixes; cleanup applies the same normalization to the relative directory path.
   */
  @Test
  public void testKillRecursively_pathWithColonsMatchesHdfsPusherLayout() throws Exception
  {
    final File testRoot = FileUtils.createTempDir();
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return testRoot.getAbsolutePath();
          }
        }
    );

    final FileSystem fs = FileSystem.get(config);
    try {
      final String relativePathWithColons =
          "batch/index_parallel_opa_affiliate_ams_key_metric_hourly_ph_live_hflgnacd_2026-03-23T10:09:40.697Z";
      final String onDiskRelativePath = relativePathWithColons.replace(':', '_');
      Path batchRoot = new Path(testRoot.getAbsolutePath(), "batch");
      Path taskDir = new Path(
          testRoot.getAbsolutePath() + Path.SEPARATOR + onDiskRelativePath + Path.SEPARATOR + "leaf"
      );
      Assertions.assertTrue(fs.mkdirs(taskDir.getParent()));
      fs.createNewFile(taskDir);

      killer.killRecursively(relativePathWithColons);

      Assertions.assertFalse(fs.exists(new Path(testRoot.getAbsolutePath() + Path.SEPARATOR + onDiskRelativePath)));
      Assertions.assertTrue(fs.exists(batchRoot));
      Assertions.assertTrue(fs.delete(batchRoot, true));
    }
    finally {
      fs.delete(new Path(testRoot.getAbsolutePath()), true);
    }
  }

  @Test
  public void testKillNonZipSegment()
  {
    Throwable exception = assertThrows(SegmentLoadingException.class, () -> {
      Configuration config = new Configuration();
      HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
          config,
          new HdfsDataSegmentPusherConfig()
          {
            @Override
            public String getStorageDirectory()
            {
              return "/tmp";
            }
          }
      );
      killer.kill(getSegmentWithPath(new Path("/xxx/", "index.beep").toString()));
    });
    assertTrue(exception.getMessage().contains("Unknown file type"));
  }

  @Test
  public void testNoStorageDirectory() throws Exception
  {
    Configuration config = new Configuration();
    HdfsDataSegmentKiller killer = new HdfsDataSegmentKiller(
        config,
        new HdfsDataSegmentPusherConfig()
        {
          @Override
          public String getStorageDirectory()
          {
            return "";
          }
        }
    );

    FileSystem fs = FileSystem.get(config);
    Path dataSourceDir = new Path("/tmp/dataSourceNew");

    Path interval1Dir = new Path(dataSourceDir, "intervalNew");
    Path version11Dir = new Path(interval1Dir, "v1");

    Assertions.assertTrue(fs.mkdirs(version11Dir));
    fs.createNewFile(new Path(version11Dir, StringUtils.format("%s_index.zip", 3)));

    // 'kill' should work even if storageDirectory is not set.
    killer.kill(getSegmentWithPath(new Path(version11Dir, "3_index.zip").toString()));

    // Verify the segment no longer exists, but that its datasource directory does.
    // Then delete its datasource directory.
    Assertions.assertFalse(fs.exists(version11Dir));
    Assertions.assertFalse(fs.exists(interval1Dir));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.exists(new Path("/tmp")));
    Assertions.assertTrue(fs.exists(dataSourceDir));
    Assertions.assertTrue(fs.delete(dataSourceDir, false));

    Throwable exception = assertThrows(IllegalStateException.class, killer::killAll);
    assertTrue(exception.getMessage().contains("Cannot delete all segment files since druid.storage.storageDirectory is not set"));
  }

  private void makePartitionDirWithIndex(FileSystem fs, Path path) throws IOException
  {
    Assertions.assertTrue(fs.mkdirs(path));
    fs.createNewFile(new Path(path, "index.zip"));
  }

  private DataSegment getSegmentWithPath(String path)
  {
    return new DataSegment(
        "dataSource",
        Intervals.of("2000/3000"),
        "ver",
        ImmutableMap.of(
            "type", "hdfs",
            "path", path
        ),
        ImmutableList.of("product"),
        ImmutableList.of("visited_sum", "unique_hosts"),
        NoneShardSpec.instance(),
        9,
        12334
    );
  }
}
