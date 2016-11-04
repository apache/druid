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

package io.druid.client.client;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import io.druid.client.ImmutableSegmentLoadInfo;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.server.coordination.DruidServerMetadata;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.NoneShardSpec;
import junit.framework.Assert;
import org.joda.time.Interval;
import org.junit.Test;

import java.io.IOException;

public class ImmutableSegmentLoadInfoTest
{
  private final ObjectMapper mapper = new DefaultObjectMapper();

  @Test
  public void testSerde() throws IOException
  {
    ImmutableSegmentLoadInfo segmentLoadInfo = new ImmutableSegmentLoadInfo(
        new DataSegment(
            "test_ds",
            new Interval(
                "2011-04-01/2011-04-02"
            ),
            "v1",
            null,
            null,
            null,
            NoneShardSpec.instance(),
            0, 0
        ), Sets.newHashSet(new DruidServerMetadata("a", "host", 10, "type", "tier", 1))
    );

    ImmutableSegmentLoadInfo serde = mapper.readValue(
        mapper.writeValueAsBytes(segmentLoadInfo),
        ImmutableSegmentLoadInfo.class
    );

    Assert.assertEquals(segmentLoadInfo, serde);
  }

}
