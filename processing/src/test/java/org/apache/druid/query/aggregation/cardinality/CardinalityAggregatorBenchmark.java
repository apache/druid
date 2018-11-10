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

package org.apache.druid.query.aggregation.cardinality;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Range;
import org.apache.druid.query.ColumnSelectorPlus;
import org.apache.druid.query.aggregation.cardinality.types.CardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.aggregation.cardinality.types.StringCardinalityAggregatorColumnSelectorStrategy;
import org.apache.druid.query.dimension.DefaultDimensionSpec;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;

/**
 * TODO rewrite to use JMH and move to the benchmarks project
 */
public class CardinalityAggregatorBenchmark extends SimpleBenchmark
{
  private static final int MAX = 5_000_000;

  CardinalityBufferAggregator agg;
  List<DimensionSelector> selectorList;
  ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy>[] dimInfos;
  ByteBuffer buf;
  int pos;

  @Param({"1", "5"})
  int multivaluedSized;
  @Param({"true", "false"})
  boolean byRow;

  @Override
  protected void setUp()
  {
    Iterable<String[]> values = FluentIterable
        .from(ContiguousSet.create(Range.closedOpen(0, 500), DiscreteDomain.integers()))
        .transform(
            new Function<Integer, String[]>()
            {
              @Override
              public String[] apply(Integer input)
              {
                if (multivaluedSized == 1) {
                  return new String[]{input.toString()};
                } else {
                  String[] res = new String[multivaluedSized];
                  String value = input.toString();
                  for (int i = 0; i < multivaluedSized; ++i) {
                    res[i] = value + i;
                  }
                  return res;
                }
              }
            }
        )
        .cycle()
        .limit(MAX);

    final DimensionSpec dimSpec1 = new DefaultDimensionSpec("dim1", "dim1");
    final CardinalityAggregatorTest.TestDimensionSelector dim1 =
        new CardinalityAggregatorTest.TestDimensionSelector(values, null);
    final ColumnSelectorPlus<CardinalityAggregatorColumnSelectorStrategy> dimInfo1 = new ColumnSelectorPlus(
        dimSpec1.getDimension(),
        dimSpec1.getOutputName(),
        new StringCardinalityAggregatorColumnSelectorStrategy(),
        dim1
    );

    selectorList = Collections.singletonList((DimensionSelector) dim1);

    dimInfos = new ColumnSelectorPlus[] {dimInfo1};

    agg = new CardinalityBufferAggregator(dimInfos, byRow);

    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        Collections.singletonList(new DefaultDimensionSpec("dim1", "dim1")),
        byRow
    );

    int maxSize = factory.getMaxIntermediateSizeWithNulls();
    buf = ByteBuffer.allocate(maxSize + 64);
    pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);
  }

  @SuppressWarnings("unused") // Supposedly called by Caliper
  public Object timeBufferAggregate(int reps)
  {
    for (int i = 0; i < reps; ++i) {
      agg.aggregate(buf, pos);

      for (final DimensionSelector selector : selectorList) {
        if (i % (MAX - 1) == 0) {
          ((CardinalityAggregatorTest.TestDimensionSelector) selector).reset();
        } else {
          ((CardinalityAggregatorTest.TestDimensionSelector) selector).increment();
        }
      }
    }
    return agg.get(buf, pos);
  }


  @Override
  protected void tearDown()
  {

  }

  public static void main(String[] args)
  {
    Runner.main(CardinalityAggregatorBenchmark.class, args);
  }
}
