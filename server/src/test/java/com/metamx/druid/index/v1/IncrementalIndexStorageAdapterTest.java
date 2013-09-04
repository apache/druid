/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
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

package com.metamx.druid.index.v1;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.metamx.common.guava.Sequence;
import com.metamx.common.guava.Sequences;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.CountAggregatorFactory;
import com.metamx.druid.aggregation.LongSumAggregatorFactory;
import com.metamx.druid.collect.StupidPool;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.input.MapBasedRow;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.group.GroupByQuery;
import com.metamx.druid.query.group.GroupByQueryEngine;
import com.metamx.druid.query.group.GroupByQueryEngineConfig;
import junit.framework.Assert;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;

/**
 */
public class IncrementalIndexStorageAdapterTest
{
  @Test
  public void testSanity() throws Exception
  {
    IncrementalIndex index = new IncrementalIndex(
        0, QueryGranularity.MINUTE, new AggregatorFactory[]{new CountAggregatorFactory("cnt")}
    );

    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("billy"),
            ImmutableMap.<String, Object>of("billy", "hi")
        )
    );
    index.add(
        new MapBasedInputRow(
            new DateTime().minus(1).getMillis(),
            Lists.newArrayList("sally"),
            ImmutableMap.<String, Object>of("sally", "bo")
        )
    );

    GroupByQueryEngine engine = new GroupByQueryEngine(
        new GroupByQueryEngineConfig()
        {
          @Override
          public int getMaxIntermediateRows()
          {
            return 5;
          }
        },
        new StupidPool<ByteBuffer>(
            new Supplier<ByteBuffer>()
            {
              @Override
              public ByteBuffer get()
              {
                return ByteBuffer.allocate(50000);
              }
            }
        )
    );

    final Sequence<Row> rows = engine.process(
        GroupByQuery.builder()
                    .setDataSource("test")
                    .setGranularity(QueryGranularity.ALL)
                    .setInterval(new Interval(0, new DateTime().getMillis()))
                    .addDimension("billy")
                    .addDimension("sally")
                    .addAggregator(new LongSumAggregatorFactory("cnt", "cnt"))
                    .build(),
        new IncrementalIndexStorageAdapter(index)
    );

    final ArrayList<Row> results = Sequences.toList(rows, Lists.<Row>newArrayList());

    Assert.assertEquals(2, results.size());

    MapBasedRow row = (MapBasedRow) results.get(0);
    Assert.assertEquals(ImmutableMap.of("billy", "hi", "cnt", 1l), row.getEvent());

    row = (MapBasedRow) results.get(1);
    Assert.assertEquals(ImmutableMap.of("sally", "bo", "cnt", 1l), row.getEvent());
  }

}
