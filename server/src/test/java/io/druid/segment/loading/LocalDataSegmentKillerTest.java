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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.UUID;

public class LocalDataSegmentKillerTest
{

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testKill() throws Exception
  {
    LocalDataSegmentKiller killer = new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig());

    // Create following segments and then delete them in this order and assert directory deletions
    // /tmp/dataSource/interval1/v1/0/index.zip
    // /tmp/dataSource/interval1/v1/1/index.zip
    // /tmp/dataSource/interval1/v2/0/index.zip
    // /tmp/dataSource/interval2/v1/0/index.zip

    final File dataSourceDir = temporaryFolder.newFolder();

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

    killer.kill(getSegmentWithPath(new File(partition011Dir, "index.zip").toString()));

    Assert.assertFalse(partition011Dir.exists());
    Assert.assertTrue(partition111Dir.exists());
    Assert.assertTrue(partition021Dir.exists());
    Assert.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(new File(partition111Dir, "index.zip").toString()));

    Assert.assertFalse(version11Dir.exists());
    Assert.assertTrue(partition021Dir.exists());
    Assert.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(new File(partition021Dir, "index.zip").toString()));

    Assert.assertFalse(interval1Dir.exists());
    Assert.assertTrue(partition012Dir.exists());

    killer.kill(getSegmentWithPath(new File(partition012Dir, "index.zip").toString()));

    Assert.assertFalse(dataSourceDir.exists());
  }

  @Test
  public void testKillUniquePath() throws Exception
  {
    final LocalDataSegmentKiller killer = new LocalDataSegmentKiller(new LocalDataSegmentPusherConfig());
    final String uuid = UUID.randomUUID().toString().substring(0, 5);
    final File dataSourceDir = temporaryFolder.newFolder("dataSource");
    final File intervalDir = new File(dataSourceDir, "interval");
    final File versionDir = new File(intervalDir, "1");
    final File partitionDir = new File(versionDir, "0");
    final File uuidDir = new File(partitionDir, uuid);

    makePartitionDirWithIndex(uuidDir);

    killer.kill(getSegmentWithPath(new File(uuidDir, "index.zip").toString()));

    Assert.assertFalse(uuidDir.exists());
    Assert.assertFalse(partitionDir.exists());
    Assert.assertFalse(versionDir.exists());
    Assert.assertFalse(intervalDir.exists());
    Assert.assertFalse(dataSourceDir.exists());
  }

  private void makePartitionDirWithIndex(File path) throws IOException
  {
    Assert.assertTrue(path.mkdirs());
    Assert.assertTrue(new File(path, "index.zip").createNewFile());
  }

  private DataSegment getSegmentWithPath(String path)
  {
    return new DataSegment(
        "dataSource",
        Intervals.of("2000/3000"),
        "ver",
        ImmutableMap.<String, Object>of(
            "type", "local",
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
