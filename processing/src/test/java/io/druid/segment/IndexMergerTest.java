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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.segment.column.Column;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.incremental.IncrementalIndex;
import io.druid.segment.incremental.OnheapIncrementalIndex;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 */
public class IndexMergerTest
{
  static {

  }

  @Test
  public void testPersist() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createIndex(true, null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist);

    final File tempDir = Files.createTempDir();
    try {
      QueryableIndex index = IndexIO.loadIndex(IndexMerger.persist(toPersist, tempDir));

      Assert.assertEquals(2, index.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
      Assert.assertEquals(3, index.getColumnNames().size());
    }
    finally {
      tempDir.delete();
    }
  }

  @Test
  public void testPersistMerge() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createIndex(true, null);
    IncrementalIndexTest.populateIndex(timestamp, toPersist1);

    IncrementalIndex toPersist2 = new OnheapIncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{new CountAggregatorFactory("count")}, 1000);

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dim1", "dim2"),
            ImmutableMap.<String, Object>of("dim1", "5", "dim2", "6")
        )
    );

    final File tempDir1 = Files.createTempDir();
    final File tempDir2 = Files.createTempDir();
    final File mergedDir = Files.createTempDir();
    try {
      QueryableIndex index1 = IndexIO.loadIndex(IndexMerger.persist(toPersist1, tempDir1));

      Assert.assertEquals(2, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
      Assert.assertEquals(3, index1.getColumnNames().size());

      QueryableIndex index2 = IndexIO.loadIndex(IndexMerger.persist(toPersist2, tempDir2));

      Assert.assertEquals(2, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
      Assert.assertEquals(3, index2.getColumnNames().size());

      QueryableIndex merged = IndexIO.loadIndex(
          IndexMerger.mergeQueryableIndex(
              Arrays.asList(index1, index2),
              new AggregatorFactory[]{new CountAggregatorFactory("count")},
              mergedDir
          )
      );

      Assert.assertEquals(3, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
      Assert.assertEquals(3, merged.getColumnNames().size());
    }
    finally {
      FileUtils.deleteQuietly(tempDir1);
      FileUtils.deleteQuietly(tempDir2);
      FileUtils.deleteQuietly(mergedDir);
    }
}

  @Test
  public void testPersistEmptyColumn() throws Exception
  {
    final IncrementalIndex toPersist1 = new OnheapIncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{}, 10);
    final IncrementalIndex toPersist2 = new OnheapIncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{}, 10);
    final File tmpDir1 = Files.createTempDir();
    final File tmpDir2 = Files.createTempDir();
    final File tmpDir3 = Files.createTempDir();

    try {
      toPersist1.add(
          new MapBasedInputRow(
              1L,
              ImmutableList.of("dim1", "dim2"),
              ImmutableMap.<String, Object>of("dim1", ImmutableList.of(), "dim2", "foo")
          )
      );

      toPersist2.add(
          new MapBasedInputRow(
              1L,
              ImmutableList.of("dim1", "dim2"),
              ImmutableMap.<String, Object>of("dim1", ImmutableList.of(), "dim2", "bar")
          )
      );

      final QueryableIndex index1 = IndexIO.loadIndex(IndexMerger.persist(toPersist1, tmpDir1));
      final QueryableIndex index2 = IndexIO.loadIndex(IndexMerger.persist(toPersist1, tmpDir2));
      final QueryableIndex merged = IndexIO.loadIndex(
          IndexMerger.mergeQueryableIndex(Arrays.asList(index1, index2), new AggregatorFactory[]{}, tmpDir3)
      );

      Assert.assertEquals(1, index1.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index1.getAvailableDimensions()));

      Assert.assertEquals(1, index2.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index2.getAvailableDimensions()));

      Assert.assertEquals(1, merged.getColumn(Column.TIME_COLUMN_NAME).getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(merged.getAvailableDimensions()));
    }
    finally {
      FileUtils.deleteQuietly(tmpDir1);
      FileUtils.deleteQuietly(tmpDir2);
      FileUtils.deleteQuietly(tmpDir3);
    }
  }
}
