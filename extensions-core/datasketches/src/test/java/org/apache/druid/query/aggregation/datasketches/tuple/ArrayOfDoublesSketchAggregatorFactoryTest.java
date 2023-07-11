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

import com.google.common.collect.ImmutableList;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketch;
import org.apache.datasketches.tuple.arrayofdoubles.ArrayOfDoublesUpdatableSketchBuilder;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.AggregateCombiner;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.TestObjectColumnSelector;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Assert;
import org.junit.Test;

public class ArrayOfDoublesSketchAggregatorFactoryTest
{

  @Test
  public void makeAggregateCombiner()
  {
    AggregatorFactory aggregatorFactory = new ArrayOfDoublesSketchAggregatorFactory("", "", null, null, null);
    AggregatorFactory combiningFactory = aggregatorFactory.getCombiningFactory();
    AggregateCombiner<ArrayOfDoublesSketch> combiner = combiningFactory.makeAggregateCombiner();

    ArrayOfDoublesUpdatableSketch sketch1 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch1.update("a", new double[] {1});

    ArrayOfDoublesUpdatableSketch sketch2 = new ArrayOfDoublesUpdatableSketchBuilder().build();
    sketch2.update("b", new double[] {1});
    sketch2.update("c", new double[] {1});

    TestObjectColumnSelector<ArrayOfDoublesSketch> selector = new TestObjectColumnSelector<ArrayOfDoublesSketch>(new ArrayOfDoublesSketch[] {sketch1, sketch2});

    combiner.reset(selector);
    Assert.assertEquals(1, combiner.getObject().getEstimate(), 0);

    selector.increment();
    combiner.fold(selector);
    Assert.assertEquals(3, combiner.getObject().getEstimate(), 0);
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ArrayOfDoublesSketchAggregatorFactory.class)
                  .withNonnullFields("name", "fieldName")
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
                  new CountAggregatorFactory("count"),
                  new ArrayOfDoublesSketchAggregatorFactory(
                      "arrayOfDoublesSketch",
                      "col",
                      1,
                      ImmutableList.of("met"),
                      1
                  )
              )
              .postAggregators(
                  new FieldAccessPostAggregator("a", "arrayOfDoublesSketch"),
                  new FinalizingFieldAccessPostAggregator("b", "arrayOfDoublesSketch")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("arrayOfDoublesSketch", null)
                    .add("a", ArrayOfDoublesSketchModule.BUILD_TYPE)
                    .add("b", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testWithName()
  {
    AggregatorFactory factory = new ArrayOfDoublesSketchAggregatorFactory("name", "", null, null, null);
    Assert.assertEquals(factory, factory.withName("name"));
    Assert.assertEquals("newTest", factory.withName("newTest").getName());
  }
}
