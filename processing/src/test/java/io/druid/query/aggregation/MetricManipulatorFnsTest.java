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

package io.druid.query.aggregation;

import io.druid.hll.HyperLogLogCollector;
import io.druid.java.util.common.StringUtils;
import io.druid.query.aggregation.hyperloglog.HyperUniquesAggregatorFactory;
import io.druid.segment.TestLongColumnSelector;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;

@RunWith(Parameterized.class)
public class MetricManipulatorFnsTest
{
  private static final String NAME = "name";
  private static final String FIELD = "field";

  @Parameterized.Parameters(name = "{0}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final ArrayList<Object[]> constructorArrays = new ArrayList<>();
    final long longVal = 13789;
    LongMinAggregator longMinAggregator = new LongMinAggregator(
        new TestLongColumnSelector()
        {
          @Override
          public long get()
          {
            return longVal;
          }
        }
    );
    LongMinAggregatorFactory longMinAggregatorFactory = new LongMinAggregatorFactory(NAME, FIELD);
    constructorArrays.add(
        new Object[]{
            longMinAggregatorFactory,
            longMinAggregator,
            longMinAggregator,
            longMinAggregator,
            longVal,
            longVal
        }
    );

    HyperUniquesAggregatorFactory hyperUniquesAggregatorFactory = new HyperUniquesAggregatorFactory(NAME, FIELD);
    HyperLogLogCollector collector = HyperLogLogCollector.makeLatestCollector();
    collector.add((short) 1, (byte) 5);

    constructorArrays.add(
        new Object[]{
            hyperUniquesAggregatorFactory,
            collector,
            collector,
            collector.estimateCardinality(),
            collector.toByteArray(),
            collector
        }
    );


    LongSumAggregatorFactory longSumAggregatorFactory = new LongSumAggregatorFactory(NAME, FIELD);
    LongSumAggregator longSumAggregator = new LongSumAggregator(
        new TestLongColumnSelector()
        {
          @Override
          public long get()
          {
            return longVal;
          }
        }
    );
    constructorArrays.add(
        new Object[]{
            longSumAggregatorFactory,
            longSumAggregator,
            longSumAggregator,
            longSumAggregator,
            longVal,
            longVal
        }
    );


    for (Object[] argList : constructorArrays) {
      Assert.assertEquals(
          StringUtils.format(
              "Arglist %s is too short. Expected 6 found %d",
              Arrays.toString(argList),
              argList.length
          ), 6, argList.length
      );
    }
    return constructorArrays;
  }

  private final AggregatorFactory aggregatorFactory;
  private final Object agg;
  private final Object identity;
  private final Object finalize;
  private final Object serialForm;
  private final Object deserForm;

  public MetricManipulatorFnsTest(
      AggregatorFactory aggregatorFactory,
      Object agg,
      Object identity,
      Object finalize,
      Object serialForm,
      Object deserForm
  )
  {
    this.aggregatorFactory = aggregatorFactory;
    this.agg = agg;
    this.identity = identity;
    this.finalize = finalize;
    this.serialForm = serialForm;
    this.deserForm = deserForm;
  }

  @Test
  public void testIdentity()
  {
    Assert.assertEquals(identity, agg);
    Assert.assertEquals(identity, MetricManipulatorFns.identity().manipulate(aggregatorFactory, agg));
  }

  @Test
  public void testFinalize()
  {
    Assert.assertEquals(identity, agg);
    Assert.assertEquals(finalize, MetricManipulatorFns.finalizing().manipulate(aggregatorFactory, agg));
  }

  @Test
  public void testDeserialize()
  {
    Assert.assertEquals(identity, agg);
    Assert.assertEquals(deserForm, MetricManipulatorFns.deserializing().manipulate(aggregatorFactory, serialForm));
  }
}
