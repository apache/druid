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

package org.apache.druid.query.aggregation.datasketches.tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;
import java.util.Map;

public class ArrayOfDoublesSketchTTestPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConstructorTooFewPostAggInputs()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Illegal number of fields[1], must be 2");
    new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        ImmutableList.of(new ConstantPostAggregator("", 0))
    );
  }

  @Test
  public void testConstructorTooManyPostAggInputs()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Illegal number of fields[3], must be 2");
    new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        Arrays.asList(
            new ConstantPostAggregator("", 0),
            new ConstantPostAggregator("", 0),
            new ConstantPostAggregator("", 0)
        )
    );
  }

  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        Arrays.asList(new ConstantPostAggregator("", 0), new ConstantPostAggregator("", 0))
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchTTestPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchTTestPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        Arrays.asList(new ConstantPostAggregator("", 0), new ConstantPostAggregator("", 0))
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchTTestPostAggregator{name='a', fields=[ConstantPostAggregator{name='', constantValue=0}, ConstantPostAggregator{name='', constantValue=0}]}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Comparing arrays of p values is not supported");
    PostAggregator postAgg = new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        Arrays.asList(new ConstantPostAggregator("", 0), new ConstantPostAggregator("", 0))
    );
    postAgg.getComparator();
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchTTestPostAggregator.class)
                  .withNonnullFields("name", "fields")
                  .withIgnoredFields("dependentFields")
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testComputeMismatchedSketches()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Sketches have different number of values: 2 and 100");
    ArrayOfDoublesUpdatableSketch s1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();
    ArrayOfDoublesUpdatableSketch s2 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(100)
                                                                                 .build();
    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(s2).anyTimes();
    EasyMock.replay(field1, field2);
    new ArrayOfDoublesSketchTTestPostAggregator(
        "a",
        Arrays.asList(field1, field2)
    ).compute(ImmutableMap.of());
  }

  @Test
  public void testResultArraySignature()
  {
    final TimeseriesQuery query =
        Druids.newTimeseriesQueryBuilder()
              .dataSource("dummy")
              .intervals("2000/3000")
              .granularity(Granularities.HOUR)
              .aggregators(
                  new CountAggregatorFactory("count")
              )
              .postAggregators(
                  new ArrayOfDoublesSketchTTestPostAggregator(
                      "a",
                      ImmutableList.of(
                          new ConstantPostAggregator("", 0),
                          new ConstantPostAggregator("", 0)
                      )
                  )
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ValueType.LONG)
                    .add("a", ValueType.DOUBLE_ARRAY)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
