/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.segment.loading;

import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import io.druid.timeline.DataSegment;
import org.apache.commons.io.FileUtils;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

public class LocalDataSegmentKillerTest
{

  private File tmpDir;
  private File segmentDir;

  private LocalDataSegmentKiller killer;
  private DataSegment segment;

  @Before
  public void setUp() throws IOException
  {
    tmpDir = Files.createTempDir();
    tmpDir.deleteOnExit();
    segmentDir = new File(
        tmpDir.getCanonicalPath()
        + "/druid/localStorage/wikipedia/2015-04-09T15:02:00.000Z_2015-04-09T15:03:00.000Z/2015-04-09T15:02:00.000Z/0/"
    );
    segmentDir.mkdirs();
    Files.touch(new File(segmentDir.getCanonicalPath() + "/index.zip"));
    Files.touch(new File(segmentDir.getCanonicalPath() + "/description.json"));

    killer = new LocalDataSegmentKiller();
    segment = new DataSegment(
        "test",
        new Interval("2015-04-09T15:02:00.000Z/2015-04-09T15:03:00.000Z"),
        "1",
        ImmutableMap.<String, Object>of("path", segmentDir.getCanonicalPath() + "/index.zip"),
        Arrays.asList("d"),
        Arrays.asList("m"),
        null,
        null,
        1234L
    );
  }

  @After
  public void tearDown() throws Exception
  {
    FileUtils.deleteDirectory(tmpDir);
  }

  @Test
  public void testKill() throws SegmentLoadingException
  {
    killer.kill(segment);
    Assert.assertTrue(!segmentDir.exists());
  }
}
