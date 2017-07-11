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

package io.druid.query;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.server.coordination.ServerType;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 */
public class LocatedSegmentDescriptorSerdeTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testDimensionsSpecSerde() throws Exception
  {
    LocatedSegmentDescriptor expected = new LocatedSegmentDescriptor(
        new SegmentDescriptor(new Interval(100, 200), "version", 100),
        65535,
        Arrays.asList(
            new DruidServerMetadata("server1", "host1", null, 30000L, ServerType.HISTORICAL, "tier1", 0),
            new DruidServerMetadata("server2", "host2", null, 40000L, ServerType.HISTORICAL, "tier1", 1),
            new DruidServerMetadata("server3", "host3", null, 50000L, ServerType.REALTIME, "tier2", 2)
        )
    );

    LocatedSegmentDescriptor actual = mapper.readValue(
        mapper.writeValueAsString(expected),
        LocatedSegmentDescriptor.class
    );

    Assert.assertEquals(expected, actual);
  }
}
