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

import java.util.Arrays;
import java.util.Map;

public class ArrayOfDoublesSketchToMetricsEstimatePostAggregatorTest
{
  @Test
  public void testSerde() throws JsonProcessingException
  {
    final PostAggregator there = new ArrayOfDoublesSketchToMetricsEstimatePostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ArrayOfDoublesSketchToMetricsEstimatePostAggregator andBackAgain = mapper.readValue(
        mapper.writeValueAsString(there),
        ArrayOfDoublesSketchToMetricsEstimatePostAggregator.class
    );

    Assert.assertEquals(there, andBackAgain);
    Assert.assertArrayEquals(there.getCacheKey(), andBackAgain.getCacheKey());
  }

  @Test
  public void testToString()
  {
    PostAggregator postAgg = new ArrayOfDoublesSketchToMetricsEstimatePostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );

    Assert.assertEquals(
        "ArrayOfDoublesSketchToMetricsEstimatePostAggregator{name='a', field=ConstantPostAggregator{name='', constantValue=0}}",
        postAgg.toString()
    );
  }

  @Test
  public void testCompute()
  {
    ArrayOfDoublesUpdatableSketch s1 = new ArrayOfDoublesUpdatableSketchBuilder().setNominalEntries(16)
        .setNumberOfValues(2)
        .build();

    s1.update("key1", new double[] {1.0, 2.0});
    s1.update("key2", new double[] {1.0, 2.0});
    s1.update("key3", new double[] {1.0, 2.0});
    s1.update("key4", new double[] {1.0, 2.0});

    PostAggregator field1 = EasyMock.createMock(PostAggregator.class);
    EasyMock.expect(field1.compute(EasyMock.anyObject(Map.class))).andReturn(s1).anyTimes();
    EasyMock.replay(field1);

    final PostAggregator postAgg = new ArrayOfDoublesSketchToMetricsEstimatePostAggregator(
        "a",
        field1
    );
    double[] expectedOutput = {4.0, 8.0};
    Assert.assertTrue(Arrays.equals(expectedOutput, (double[]) postAgg.compute(ImmutableMap.of())));
  }


  @Test
  public void testComparator()
  {
    final PostAggregator postAgg = new ArrayOfDoublesSketchToMetricsEstimatePostAggregator(
        "a",
        new ConstantPostAggregator("", 0)
    );
    Exception exception = Assert.assertThrows(IAE.class, () -> postAgg.getComparator());
    Assert.assertEquals("Comparing arrays of estimate values is not supported", exception.getMessage());
  }

  @Test
  public void testEqualsAndHashCode()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchToMetricsEstimatePostAggregator.class)
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
                new ArrayOfDoublesSketchToMetricsEstimatePostAggregator(
                    "a",
                    new ConstantPostAggregator("", 0)
                )
            )
            .build();

    Assert.assertEquals(
        RowSignature.builder()
            .addTimeColumn()
            .add("count", ColumnType.LONG)
            .add("a", ColumnType.DOUBLE_ARRAY)
            .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }
}
