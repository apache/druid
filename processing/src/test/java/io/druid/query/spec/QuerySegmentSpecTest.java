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

package io.druid.query.spec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.SegmentDescriptor;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 */
public class QuerySegmentSpecTest
{
  private static final ObjectMapper jsonMapper = new DefaultObjectMapper();

  @Test
  public void testSerializationLegacyString() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "\"2011-10-01/2011-10-10,2011-11-01/2011-11-10\"", QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof LegacySegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-10-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationLegacyArray() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "[\"2011-09-01/2011-10-10\", \"2011-11-01/2011-11-10\"]", QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof LegacySegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-09-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationIntervals() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.readValue(
        "{\"type\": \"intervals\", \"intervals\":[\"2011-08-01/2011-10-10\", \"2011-11-01/2011-11-10\"]}",
        QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof MultipleIntervalSegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-08-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
  }

  @Test
  public void testSerializationSegments() throws Exception
  {
    QuerySegmentSpec spec = jsonMapper.convertValue(
        ImmutableMap.<String, Object>of(
            "type", "segments",

            "segments", ImmutableList
            .<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-07-01/2011-10-10",
                    "ver", "1",
                    "part", 0
                ),
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-07-01/2011-10-10",
                    "ver", "1",
                    "part", 1
                ),
                ImmutableMap.<String, Object>of(
                    "itvl", "2011-11-01/2011-11-10",
                    "ver", "2",
                    "part", 10
                )
            )
        ),
        QuerySegmentSpec.class
    );
    Assert.assertTrue(spec instanceof MultipleSpecificSegmentSpec);
    Assert.assertEquals(
        ImmutableList.of(new Interval("2011-07-01/2011-10-10"), new Interval("2011-11-01/2011-11-10")),
        spec.getIntervals()
    );
    Assert.assertEquals(
        ImmutableList.of(
            new SegmentDescriptor(new Interval("2011-07-01/2011-10-10"), "1", 0),
            new SegmentDescriptor(new Interval("2011-07-01/2011-10-10"), "1", 1),
            new SegmentDescriptor(new Interval("2011-11-01/2011-11-10"), "2", 10)
        ),
        ((MultipleSpecificSegmentSpec) spec).getDescriptors()
    );
  }
}
