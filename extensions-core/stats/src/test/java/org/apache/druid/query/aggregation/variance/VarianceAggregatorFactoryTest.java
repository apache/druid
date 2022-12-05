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

package org.apache.druid.query.aggregation.variance;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Druids;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.aggregation.post.FinalizingFieldAccessPostAggregator;
import org.apache.druid.query.timeseries.TimeseriesQuery;
import org.apache.druid.query.timeseries.TimeseriesQueryQueryToolChest;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Test;

public class VarianceAggregatorFactoryTest extends InitializedNullHandlingTest
{
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
                  new VarianceAggregatorFactory("variance", "col"),
                  new VarianceFoldingAggregatorFactory("varianceFold", "col", null)
              )
              .postAggregators(
                  new FieldAccessPostAggregator("variance-access", "variance"),
                  new FinalizingFieldAccessPostAggregator("variance-finalize", "variance"),
                  new FieldAccessPostAggregator("varianceFold-access", "varianceFold"),
                  new FinalizingFieldAccessPostAggregator("varianceFold-finalize", "varianceFold")
              )
              .build();

    Assert.assertEquals(
        RowSignature.builder()
                    .addTimeColumn()
                    .add("count", ColumnType.LONG)
                    .add("variance", null)
                    .add("varianceFold", null)
                    .add("variance-access", VarianceAggregatorFactory.TYPE)
                    .add("variance-finalize", ColumnType.DOUBLE)
                    .add("varianceFold-access", VarianceAggregatorFactory.TYPE)
                    .add("varianceFold-finalize", ColumnType.DOUBLE)
                    .build(),
        new TimeseriesQueryQueryToolChest().resultArraySignature(query)
    );
  }

  @Test
  public void testFinalizeComputationWithZeroCountShouldReturnNull()
  {
    VarianceAggregatorFactory target = new VarianceAggregatorFactory("test", "test", null, null);
    VarianceAggregatorCollector v1 = new VarianceAggregatorCollector();
    Assert.assertEquals(NullHandling.defaultDoubleValue(), target.finalizeComputation(v1));
  }

  @Test
  public void testFinalizeComputationWithNullShouldReturnNull()
  {
    VarianceAggregatorFactory target = new VarianceAggregatorFactory("test", "test", null, null);
    Assert.assertEquals(NullHandling.defaultDoubleValue(), target.finalizeComputation(null));
  }

  @Test
  public void testWithName()
  {
    VarianceAggregatorFactory varianceAggregatorFactory = new VarianceAggregatorFactory("variance", "col");
    Assert.assertEquals(varianceAggregatorFactory, varianceAggregatorFactory.withName("variance"));
    Assert.assertEquals("newTest", varianceAggregatorFactory.withName("newTest").getName());

    VarianceFoldingAggregatorFactory varianceFoldingAggregatorFactory = new VarianceFoldingAggregatorFactory(
        "varianceFold",
        "col",
        null
    );
    Assert.assertEquals(varianceFoldingAggregatorFactory, varianceFoldingAggregatorFactory.withName("varianceFold"));
    Assert.assertEquals("newTest", varianceFoldingAggregatorFactory.withName("newTest").getName());
  }
}
