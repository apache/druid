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

import com.google.common.primitives.Ints;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.mutable.MutableInt;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.SegmentId;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

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
  public void testLogNoSegments()
  {
    List<String> messages = new ArrayList<>();
    Logger.LogFunction logger = getLogToListFunction(messages);
    SegmentUtils.logSegments(logger, Collections.emptyList(), "None segments");
    SegmentUtils.logSegmentIds(logger, Stream.empty(), "None segments");

    Assert.assertEquals(0, messages.size());
  }

  @Test
  public void testLogSegments()
  {
    List<String> messages = new ArrayList<>();
    List<DataSegment> segments = makeDataSegments(2).collect(Collectors.toList());
    Logger.LogFunction logger = getLogToListFunction(messages);
    SegmentUtils.logSegments(logger, segments, "Test segments");

    Assert.assertEquals(1, messages.size());
    final String expected =
        "Test segments: [someDataSource_2012-01-01T00:00:00.000Z_2012-01-03T00:00:00.000Z_2020-02-02T00:00:00.000Z,"
        + " someDataSource_2012-01-02T00:00:00.000Z_2012-01-04T00:00:00.000Z_2020-02-02T00:00:00.000Z]";
    Assert.assertEquals(expected, messages.get(0));
  }


  @Test
  public void testLogSegmentIds()
  {
    List<String> messages = new ArrayList<>();
    Stream<SegmentId> segments = makeDataSegments(2).map(DataSegment::getId);
    Logger.LogFunction logger = getLogToListFunction(messages);
    SegmentUtils.logSegmentIds(logger, segments, "Test segments");

    Assert.assertEquals(1, messages.size());
    final String expected =
        "Test segments: [someDataSource_2012-01-01T00:00:00.000Z_2012-01-03T00:00:00.000Z_2020-02-02T00:00:00.000Z,"
        + " someDataSource_2012-01-02T00:00:00.000Z_2012-01-04T00:00:00.000Z_2020-02-02T00:00:00.000Z]";
    Assert.assertEquals(expected, messages.get(0));
  }


  @Test
  public void testLogSegmentsMany()
  {
    final int numSegments = 100000;
    final MutableInt msgCount = new MutableInt();
    final Stream<SegmentId> segments = makeDataSegments(numSegments).map(DataSegment::getId);

    final Logger.LogFunction logger = (msg, format) -> {
      String message = StringUtils.format(msg, format);
      Assert.assertTrue(message.startsWith("Many segments: ["));
      Assert.assertTrue(message.endsWith("]"));
      msgCount.increment();
    };
    SegmentUtils.logSegmentIds(logger, segments, "Many segments");

    final int expected = (int) Math.ceil((double) numSegments / SegmentUtils.SEGMENTS_PER_LOG_MESSAGE);
    Assert.assertEquals(expected, msgCount.getValue());
  }

  private Logger.LogFunction getLogToListFunction(List<String> messages)
  {
    return (msg, format) -> messages.add(StringUtils.format(msg, format));
  }

  private Stream<DataSegment> makeDataSegments(int numSegments)
  {
    final DateTime start = DateTimes.of("2012-01-01");
    final DateTime end = DateTimes.of("2012-01-02");
    final String version = DateTimes.of("2020-02-02").toString();
    return IntStream.range(0, numSegments)
                    .mapToObj(segmentNum -> DataSegment.builder()
                                                       .dataSource("someDataSource")
                                                       .interval(
                                                           new Interval(
                                                               start.plusDays(segmentNum),
                                                               end.plusDays(segmentNum + 1)
                                                           )
                                                       )
                                                       .version(version)
                                                       .size(1)
                                                       .build());

  }
}
