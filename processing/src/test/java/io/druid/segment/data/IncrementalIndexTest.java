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

package io.druid.segment.data;

import com.google.common.collect.ImmutableMap;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.Row;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.incremental.IncrementalIndex;
import junit.framework.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 */
public class IncrementalIndexTest
{

  public static IncrementalIndex createCaseInsensitiveIndex(long timestamp)
  {
    IncrementalIndex index = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("Dim1", "DiM2"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "DIM1", "3", "dIM2", "4")
        )
    );

    index.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("diM1", "dIM2"),
            ImmutableMap.<String, Object>of("Dim1", "1", "DiM2", "2", "dim1", "3", "dim2", "4")
        )
    );
    return index;
  }

  public static MapBasedInputRow getRow(long timestamp, int rowID, int dimensionCount)
  {
    List<String> dimensionList = new ArrayList<String>(dimensionCount);
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < dimensionCount; i++) {
      String dimName = String.format("Dim_%d", i);
      dimensionList.add(dimName);
      builder.put(dimName, dimName + rowID);
    }
    return new MapBasedInputRow(timestamp, dimensionList, builder.build());
  }

  @Test
  public void testCaseInsensitivity() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex index = createCaseInsensitiveIndex(timestamp);

    Assert.assertEquals(Arrays.asList("dim1", "dim2"), index.getDimensions());
    Assert.assertEquals(2, index.size());

    final Iterator<Row> rows = index.iterator();
    Row row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("1"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("2"), row.getDimension("dim2"));

    row = rows.next();
    Assert.assertEquals(timestamp, row.getTimestampFromEpoch());
    Assert.assertEquals(Arrays.asList("3"), row.getDimension("dim1"));
    Assert.assertEquals(Arrays.asList("4"), row.getDimension("dim2"));
  }

  @Test
  public void testConcurrentAdd() throws Exception
  {
    final IncrementalIndex index = new IncrementalIndex(
        0L,
        QueryGranularity.NONE,
        new AggregatorFactory[]{new CountAggregatorFactory("count")}
    );
    final int threadCount = 10;
    final int elementsPerThread = 200;
    final int dimensionCount = 5;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    final long timestamp = System.currentTimeMillis();
    final CountDownLatch latch = new CountDownLatch(threadCount);
    for (int j = 0; j < threadCount; j++) {
      executor.submit(
          new Runnable()
          {
            @Override
            public void run()
            {
              try {
                for (int i = 0; i < elementsPerThread; i++) {
                  index.add(getRow(timestamp + i, i, dimensionCount));
                }
              }
              catch (Exception e) {
                e.printStackTrace();
              }
              latch.countDown();
            }
          }
      );
    }
    Assert.assertTrue(latch.await(60, TimeUnit.SECONDS));

    Assert.assertEquals(dimensionCount, index.getDimensions().size());
    Assert.assertEquals(elementsPerThread, index.size());
    Iterator<Row> iterator = index.iterator();
    int curr = 0;
    while (iterator.hasNext()) {
      Row row = iterator.next();
      Assert.assertEquals(timestamp + curr, row.getTimestampFromEpoch());
      Assert.assertEquals(Float.valueOf(threadCount), row.getFloatMetric("count"));
      curr++;
    }
    Assert.assertEquals(elementsPerThread, curr);
  }
}
