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
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.query.aggregation.post.ConstantPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class ArrayOfDoublesSketchToBase64StringPostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchToBase64StringPostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchToBase64StringPostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchToBase64StringPostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchToBase64StringPostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchToBase64StringPostAggregator{name='a', field=ConstantPostAggregator{name='', constantValue=0}}",
        postAgg.toString()
    );
  }

  @Test
  public void testCompute()
  {
    ArrayOfDoublesUpdatableSketch s1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
        .setNumberOfValues(2)
        .build();

    s1.update("foo", new double[] {1.0, 2.0});

    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    EasyMock.replay(field1);

    final PostAggregator postAgg = new ArrayOfDoublesSketchToBase64StringPostAggregator(
        "a",
        field1
    );
    Assert.assertNotNull("output string should not be null", postAgg.compute(ImmutableMap.of()));
  }

  @Test
  public void testComparator()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchToBase64StringPostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );
    Exception exception = Assert.assertThrows(IAE.class, () -> postAgg.getComparator());
    Assert.assertEquals("Comparing sketch summaries is not supported", exception.getMessage());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchToBase64StringPostAggregator.class)
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
                new ArrayOfDoublesSketchToBase64StringPostAggregator(
                    "a",
                    new ConstantPostAggregator("", 1)
                )
            )
            .build();

    Assert.assertEquals(
        RowSignature.builder()
            .addTimeColumn()
            .add("count", ColumnType.LONG)
            .add("a", ColumnType.STRING)
            .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
