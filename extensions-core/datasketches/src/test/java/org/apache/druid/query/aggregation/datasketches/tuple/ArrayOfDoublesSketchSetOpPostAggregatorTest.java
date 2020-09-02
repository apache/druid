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
import org.apache.datasketches.tuple.ArrayOfDoublesSketch;
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
import java.util.Comparator;
import java.util.Map;

public class ArrayOfDoublesSketchSetOpPostAggregatorTest
{
  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void testConstructorNumArgs()
  {
    expectedException.expect(IAE.class);
    expectedException.expectMessage("Illegal number of fields[0], must be > 1");
    final PostAggregator there = new ArrayOfDoublesSketchSetOpPostAggregator(
        "a",
        "UNION",
        null,
        null,
        ImmutableList.of()
    );
  }
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchSetOpPostAggregator(
        "a",
        "UNION",
        null,
        null,
        Arrays.asList(new ConstantPostAggregator("", 0), new ConstantPostAggregator("", 0))
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchSetOpPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchSetOpPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg = new ArrayOfDoublesSketchSetOpPostAggregator(
        "a",
        "UNION",
        16,
        1000,
        Arrays.asList(new ConstantPostAggregator("", 0), new ConstantPostAggregator("", 0))
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchSetOpPostAggregator{name='a', fields=[ConstantPostAggregator{name='', constantValue=0}, ConstantPostAggregator{name='', constantValue=0}], operation=UNION, nominalEntries=16, numberOfValues=1000}",
        postAgg.toString()
    );
  }

  @Test
  public void testComparator()
  {
    ArrayOfDoublesUpdatableSketch s1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();

    s1.update("foo", new double[]{1.0, 2.0});
    ArrayOfDoublesUpdatableSketch s2 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();
    s2.update("foo", new double[]{2.0, 2.0});
    s2.update("bar", new double[]{3.0, 4.0});

    // duplicate
    ArrayOfDoublesUpdatableSketch s3 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();

    s3.update("foo", new double[]{1.0, 2.0});
    ArrayOfDoublesUpdatableSketch s4 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
                                                                                 .setNumberOfValues(2)
                                                                                 .build();
    s4.update("foo", new double[]{2.0, 2.0});
    s4.update("bar", new double[]{3.0, 4.0});
    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(s2).anyTimes();
    PostAggregator field3 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field3.compute(EasyMock.anyObject(Map.class))).andReturn(s3).anyTimes();
    PostAggregator field4 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field4.compute(EasyMock.anyObject(Map.class))).andReturn(s4).anyTimes();
    EasyMock.replay(field1, field2, field3, field4);

    final ArrayOfDoublesSketchSetOpPostAggregator postAgg1 = new ArrayOfDoublesSketchSetOpPostAggregator(
        "a",
        "UNION",
        16,
        2,
        ImmutableList.of(field1, field2)
    );
    final ArrayOfDoublesSketchSetOpPostAggregator postAgg2 = new ArrayOfDoublesSketchSetOpPostAggregator(
        "a",
        "UNION",
        16,
        2,
        ImmutableList.of(field3, field4)
    );
    Comparator comparator = postAgg1.getComparator();
    ArrayOfDoublesSketch sketch1 = postAgg1.compute(ImmutableMap.of());
    ArrayOfDoublesSketch sketch2 = postAgg2.compute(ImmutableMap.of());

    // comparator compares value of each sketches estimate so should be identical
    Assert.assertEquals(0, comparator.compare(sketch1, sketch2));
    Assert.assertEquals(0, Double.compare(sketch1.getEstimate(), sketch2.getEstimate()));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchSetOpPostAggregator.class)
                  .withNonnullFields("name", "fields", "operation")
                  .withIgnoredFields("dependentFields")
                  .usingGetClass()
                  .verify();
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
                  new ArrayOfDoublesSketchSetOpPostAggregator(
                      "a",
                      "UNION",
                      null,
                      null,
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
                    .add("a", ValueType.COMPLEX)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
