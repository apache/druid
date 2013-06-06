/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

import com.google.common.collect.ImmutableMap;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.input.MapBasedInputRow;
import com.metamx.druid.input.Row;
import junit.framework.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

/**
 */
public class IncrementalIndexTest
{

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
}
