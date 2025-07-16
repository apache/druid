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

package org.apache.druid.client;

import com.fasterxml.jackson.databind.InjectableValues.Std;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.test.utils.ImmutableDruidDataSourceTestUtils;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.timeline.DataSegment.PruneSpecsHolder;
import org.apache.druid.timeline.SegmentId;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;

public class ImmutableDruidDataSourceTest
{
  private static final DataSegment TEST_SEGMENT = DataSegment.builder(SegmentId.of(
                                                                 "test",
                                                                 Intervals.of("2017/2018"),
                                                                 "version",
                                                                 null
                                                             ))
                                                             .dimensions(ImmutableList.of("dim1", "dim2"))
                                                             .metrics(ImmutableList.of("met1", "met2"))
                                                             .projections(ImmutableList.of("proj1", "proj2"))
                                                             .binaryVersion(1)
                                                             .size(100L)
                                                             .build();

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testSerde() throws IOException
  {
    final ImmutableDruidDataSource dataSource = getImmutableDruidDataSource(TEST_SEGMENT);

    final ObjectMapper objectMapper = new DefaultObjectMapper()
        .setInjectableValues(new Std().addValue(PruneSpecsHolder.class, PruneSpecsHolder.DEFAULT));
    final String json = objectMapper.writeValueAsString(dataSource);

    ImmutableDruidDataSourceTestUtils.assertEquals(dataSource, objectMapper.readValue(json,
        ImmutableDruidDataSource.class));
  }

  @Test
  public void testEqualsMethodThrowsUnsupportedOperationException()
  {
    final ImmutableDruidDataSource dataSource1 = getImmutableDruidDataSource(TEST_SEGMENT);


    final ImmutableDruidDataSource dataSource2 = getImmutableDruidDataSource(TEST_SEGMENT);

    Assert.assertThrows(
        "ImmutableDruidDataSource shouldn't be used as the key in containers",
        UnsupportedOperationException.class,
        () -> dataSource1.equals(dataSource2)
    );
  }

  private static ImmutableDruidDataSource getImmutableDruidDataSource(DataSegment segment1)
  {
    return new ImmutableDruidDataSource(
        "test",
        ImmutableMap.of("prop1", "val1", "prop2", "val2"),
        ImmutableSortedMap.of(segment1.getId(), segment1)
    );
  }


  @Test
  public void testHashCodeMethodThrowsUnsupportedOperationException()
  {
    final ImmutableDruidDataSource dataSource = getImmutableDruidDataSource(TEST_SEGMENT);

    Assert.assertThrows(
        "ImmutableDruidDataSource shouldn't be used as the key in containers",
        UnsupportedOperationException.class,
        dataSource::hashCode
    );
  }
}
