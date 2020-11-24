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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.quantiles.DoublesSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
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
import org.junit.Test;

import java.util.Comparator;
import java.util.Map;

public class ArrayOfDoublesSketchToQuantilesSketchPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
        "a",
        new ConstantPostAggregator("", 0),
        null,
        null
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchToQuantilesSketchPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchToQuantilesSketchPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg = new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
        "a",
        new ConstantPostAggregator("", 0),
        2,
        16
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchToQuantilesSketchPostAggregator{name='a', field=ConstantPostAggregator{name='', constantValue=0}, column=2, k=16}",
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
    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    PostAggregator field2 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field2.compute(EasyMock.anyObject(Map.class))).andReturn(s2).anyTimes();
    EasyMock.replay(field1, field2);

    final ArrayOfDoublesSketchToQuantilesSketchPostAggregator postAgg1 = new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
        "a",
        field1,
        null,
        null
    );
    final ArrayOfDoublesSketchToQuantilesSketchPostAggregator postAgg2 = new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
        "a",
        field2,
        null,
        null
    );
    Comparator comparator = postAgg1.getComparator();
    DoublesSketch sketch1 = postAgg1.compute(ImmutableMap.of());
    DoublesSketch sketch2 = postAgg2.compute(ImmutableMap.of());

    // comparator compares value of getN, which is 1 for sketch1 and 2 for sketch2
    Assert.assertEquals(-1, comparator.compare(sketch1, sketch2));
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchToQuantilesSketchPostAggregator.class)
                  .withNonnullFields("name", "field")
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
                  new ArrayOfDoublesSketchToQuantilesSketchPostAggregator(
                      "a",
                      new ConstantPostAggregator("", 0),
                      2,
                      null
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
