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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.partition.NumberedShardSpec;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 */
public class SegmentUtilsTest
{
  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testVersionBin() throws Exception
  {
    File dir = tempFolder.newFolder();
    FileUtils.writeByteArrayToFile(new File(dir, "version.bin"), Ints.toByteArray(9));
    Assert.assertEquals(9, SegmentUtils.getVersionFromDir(dir));
  }

  @Test
  public void testIndexDrd() throws Exception
  {
    File dir = tempFolder.newFolder();
    FileUtils.writeByteArrayToFile(new File(dir, "index.drd"), new byte[]{(byte) 0x8});
    Assert.assertEquals(8, SegmentUtils.getVersionFromDir(dir));
  }

  @Test(expected = IOException.class)
  public void testException() throws Exception
  {
    SegmentUtils.getVersionFromDir(tempFolder.newFolder());
  }

  @Test
  public void testGroupSegmentsByInterval()
  {
    final List<DataSegment> segments = ImmutableList.of(
        newSegment(Intervals.of("2020-01-01/P1D"), 0),
        newSegment(Intervals.of("2020-01-02/P1D"), 0),
        newSegment(Intervals.of("2020-01-01/P1D"), 1),
        newSegment(Intervals.of("2020-01-03/P1D"), 0),
        newSegment(Intervals.of("2020-01-02/P1D"), 1),
        newSegment(Intervals.of("2020-01-02/P1D"), 2)
    );
    Assert.assertEquals(
        ImmutableMap.of(
            Intervals.of("2020-01-01/P1D"),
            ImmutableList.of(
                newSegment(Intervals.of("2020-01-01/P1D"), 0),
                newSegment(Intervals.of("2020-01-01/P1D"), 1)
            ),
            Intervals.of("2020-01-02/P1D"),
            ImmutableList.of(
                newSegment(Intervals.of("2020-01-02/P1D"), 0),
                newSegment(Intervals.of("2020-01-02/P1D"), 1),
                newSegment(Intervals.of("2020-01-02/P1D"), 2)
            ),
            Intervals.of("2020-01-03/P1D"),
            ImmutableList.of(newSegment(Intervals.of("2020-01-03/P1D"), 0))
        ),
        SegmentUtils.groupSegmentsByInterval(segments)
    );
  }

  private static DataSegment newSegment(Interval interval, int partitionId)
  {
    return new DataSegment(
        "datasource",
        interval,
        "version",
        null,
        ImmutableList.of("dim"),
        ImmutableList.of("met"),
        new NumberedShardSpec(partitionId, 0),
        null,
        9,
        10L
    );
  }
}
