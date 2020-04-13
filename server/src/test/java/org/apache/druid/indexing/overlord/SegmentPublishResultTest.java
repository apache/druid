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

package org.apache.druid.indexing.overlord;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class SegmentPublishResultTest
{
  private final ObjectMapper objectMapper = new DefaultObjectMapper()
      .setInjectableValues(new Std().addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT));

  @Test
  public void testSerdeOkResult() throws IOException
  {
    final SegmentPublishResult original = SegmentPublishResult.ok(
        ImmutableSet.of(
            segment(Intervals.of("2018/2019")),
            segment(Intervals.of("2019/2020"))
        )
    );

    final String json = objectMapper.writeValueAsString(original);
    final SegmentPublishResult fromJson = objectMapper.readValue(json, SegmentPublishResult.class);
    Assert.assertEquals(original, fromJson);
  }

  @Test
  public void testSerdeFailResult() throws IOException
  {
    final SegmentPublishResult original = SegmentPublishResult.fail("test");

    final String json = objectMapper.writeValueAsString(original);
    final SegmentPublishResult fromJson = objectMapper.readValue(json, SegmentPublishResult.class);
    Assert.assertEquals(original, fromJson);
  }

  private static DataSegment segment(Interval interval)
  {
    return new DataSegment(
        "ds",
        interval,
        "version",
        null,
        null,
        null,
        null,
        9,
        10L
    );
  }
}
