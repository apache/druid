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

package io.druid.segment;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.metamx.collections.bitmap.ConciseBitmapFactory;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.IncrementalIndexAdapter;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class EmptyIndexTest
{
  @Test
  public void testEmptyIndex() throws Exception
  {
    File tmpDir = File.createTempFile("emptyIndex", "");
    if (!tmpDir.delete()) {
      throw new IllegalStateException("tmp delete failed");
    }
    if (!tmpDir.mkdir()) {
      throw new IllegalStateException("tmp mkdir failed");
    }
    tmpDir.deleteOnExit();

    IncrementalIndex emptyIndex = new OnheapIncrementalIndex(
        0,
        QueryGranularity.NONE,
        new AggregatorFactory[0],
        1000
    );
    IncrementalIndexAdapter emptyIndexAdapter = new IncrementalIndexAdapter(
        new Interval("2012-08-01/P3D"),
        emptyIndex,
        new ConciseBitmapFactory()
    );
    IndexMerger.merge(
        Lists.<IndexableAdapter>newArrayList(emptyIndexAdapter),
        new AggregatorFactory[0],
        tmpDir
    );

    QueryableIndex emptyQueryableIndex = IndexIO.loadIndex(tmpDir);

    Assert.assertEquals("getDimensionNames", 0, Iterables.size(emptyQueryableIndex.getAvailableDimensions()));
    Assert.assertEquals("getMetricNames", 0, Iterables.size(emptyQueryableIndex.getColumnNames()));
    Assert.assertEquals("getDataInterval", new Interval("2012-08-01/P3D"), emptyQueryableIndex.getDataInterval());
    Assert.assertEquals("getReadOnlyTimestamps", 0, emptyQueryableIndex.getColumn(Column.TIME_COLUMN_NAME).getLength());
  }
}
