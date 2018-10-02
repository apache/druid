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

package org.apache.druid.segment;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.commons.io.FileUtils;
import org.apache.druid.collections.bitmap.ConciseBitmapFactory;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.incremental.IncrementalIndex;
import org.apache.druid.segment.incremental.IncrementalIndexAdapter;
import org.apache.druid.segment.writeout.OffHeapMemorySegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.SegmentWriteOutMediumFactory;
import org.apache.druid.segment.writeout.TmpFileSegmentWriteOutMediumFactory;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Collection;
import java.util.Collections;

@RunWith(Parameterized.class)
public class EmptyIndexTest
{

  @Parameterized.Parameters
  public static Collection<?> constructorFeeder()
  {
    return ImmutableList.of(
        new Object[] {TmpFileSegmentWriteOutMediumFactory.instance()},
        new Object[] {OffHeapMemorySegmentWriteOutMediumFactory.instance()}
    );
  }

  private final SegmentWriteOutMediumFactory segmentWriteOutMediumFactory;

  public EmptyIndexTest(SegmentWriteOutMediumFactory segmentWriteOutMediumFactory)
  {
    this.segmentWriteOutMediumFactory = segmentWriteOutMediumFactory;
  }

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

    try {
      IncrementalIndex emptyIndex = new IncrementalIndex.Builder()
          .setSimpleTestingIndexSchema(/* empty */)
          .setMaxRowCount(1000)
          .buildOnheap();

      IncrementalIndexAdapter emptyIndexAdapter = new IncrementalIndexAdapter(
          Intervals.of("2012-08-01/P3D"),
          emptyIndex,
          new ConciseBitmapFactory()
      );
      TestHelper.getTestIndexMergerV9(segmentWriteOutMediumFactory).merge(
          Collections.singletonList(emptyIndexAdapter),
          true,
          new AggregatorFactory[0],
          tmpDir,
          new IndexSpec()
      );

      QueryableIndex emptyQueryableIndex = TestHelper.getTestIndexIO().loadIndex(tmpDir);

      Assert.assertEquals("getDimensionNames", 0, Iterables.size(emptyQueryableIndex.getAvailableDimensions()));
      Assert.assertEquals("getMetricNames", 0, emptyQueryableIndex.getColumnNames().size());
      Assert.assertEquals("getDataInterval", Intervals.of("2012-08-01/P3D"), emptyQueryableIndex.getDataInterval());
      Assert.assertEquals(
          "getReadOnlyTimestamps",
          0,
          emptyQueryableIndex.getColumnHolder(ColumnHolder.TIME_COLUMN_NAME).getLength()
      );
    }
    finally {
      FileUtils.deleteDirectory(tmpDir);
    }
  }
}
