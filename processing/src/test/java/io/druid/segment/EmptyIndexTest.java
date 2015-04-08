/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
        tmpDir,
        new IndexSpec()
    );

    QueryableIndex emptyQueryableIndex = IndexIO.loadIndex(tmpDir);

    Assert.assertEquals("getDimensionNames", 0, Iterables.size(emptyQueryableIndex.getAvailableDimensions()));
    Assert.assertEquals("getMetricNames", 0, Iterables.size(emptyQueryableIndex.getColumnNames()));
    Assert.assertEquals("getDataInterval", new Interval("2012-08-01/P3D"), emptyQueryableIndex.getDataInterval());
    Assert.assertEquals("getReadOnlyTimestamps", 0, emptyQueryableIndex.getColumn(Column.TIME_COLUMN_NAME).getLength());
  }
}
