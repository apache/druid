/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013, 2014  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.query.aggregation.cardinality;

import com.google.caliper.Param;
import com.google.caliper.Runner;
import com.google.caliper.SimpleBenchmark;
import com.google.common.base.Function;
import com.google.common.collect.ContiguousSet;
import com.google.common.collect.DiscreteDomain;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import io.druid.segment.DimensionSelector;

import java.nio.ByteBuffer;
import java.util.List;

public class CardinalityAggregatorBenchmark extends SimpleBenchmark
{
  private final static int MAX = 5_000_000;

  CardinalityBufferAggregator agg;
  List<DimensionSelector> selectorList;
  ByteBuffer buf;
  int pos;

  @Param({"1", "5"})
  int multivaluedSized;
  @Param({"true", "false"})
  boolean byRow;

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


    final CardinalityAggregatorTest.TestDimensionSelector dim1 =
        new CardinalityAggregatorTest.TestDimensionSelector(values);

    selectorList = Lists.newArrayList(
        (DimensionSelector) dim1
    );

    agg = new CardinalityBufferAggregator(
        selectorList,
        byRow
    );

    CardinalityAggregatorFactory factory = new CardinalityAggregatorFactory(
        "billy",
        Lists.newArrayList("dim1"),
        byRow
    );

    int maxSize = factory.getMaxIntermediateSize();
    buf = ByteBuffer.allocate(maxSize + 64);
    pos = 10;
    buf.limit(pos + maxSize);

    agg.init(buf, pos);
  }

  public Object timeBufferAggregate(int reps) throws Exception
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

  public static void main(String[] args) throws Exception
  {
    Runner.main(CardinalityAggregatorBenchmark.class, args);
  }
}
