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

package org.apache.druid.indexing.common.task;

import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class SpecificSegmentsSpecTest
{
  @Test
  public void createTest()
  {
    final List<DataSegment> segments = IntStream
        .range(0, 20)
        .mapToObj(i -> newSegment(Intervals.of("2019-01-%02d/2019-01-%02d", i + 1, i + 2)))
        .collect(Collectors.toList());
    final List<String> expectedSegmentIds = segments
        .stream()
        .map(segment -> segment.getId().toString())
        .collect(Collectors.toList());
    Collections.shuffle(segments, ThreadLocalRandom.current());
    final SpecificSegmentsSpec spec = SpecificSegmentsSpec.fromSegments(segments);
    Assert.assertEquals(expectedSegmentIds, spec.getSegments());
  }

  private static DataSegment newSegment(Interval interval)
  {
    return new DataSegment(
        "datasource",
        interval,
        "version",
        null,
        null,
        null,
        null,
        9,
        10
    );
  }
}
