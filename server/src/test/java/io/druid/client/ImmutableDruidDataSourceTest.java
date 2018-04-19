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

package io.druid.client;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Intervals;
import io.druid.timeline.DataSegment;
import io.druid.timeline.DataSegment.PruneLoadSpecHolder;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ImmutableDruidDataSourceTest
{
  @Test
  public void testSerde() throws IOException
  {
    final DataSegment segment = new DataSegment(
        "test",
        Intervals.of("2017/2018"),
        "version",
        null,
        ImmutableList.of("dim1", "dim2"),
        ImmutableList.of("met1", "met2"),
        null,
        1,
        100L,
        PruneLoadSpecHolder.DEFAULT
    );
    final ImmutableDruidDataSource dataSource = new ImmutableDruidDataSource(
        "test",
        ImmutableMap.of("prop1", "val1", "prop2", "val2"),
        ImmutableSortedMap.of(segment.getIdentifier(), segment)
    );

    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .setInjectableValues(new Std().addValue(PruneLoadSpecHolder.class, PruneLoadSpecHolder.DEFAULT));
    final String json = objectMapper.writeValueAsString(dataSource);
    Assert.assertEquals(dataSource, objectMapper.readValue(json, ImmutableDruidDataSource.class));
  }
}
