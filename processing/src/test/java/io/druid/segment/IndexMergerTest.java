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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import io.druid.data.input.MapBasedInputRow;
import io.druid.granularity.QueryGranularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.data.IncrementalIndexTest;
import io.druid.segment.incremental.IncrementalIndex;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

/**
 */
public class IndexMergerTest
{
  @Test
  public void testPersistCaseInsensitive() throws Exception
  {
    final long timestamp = System.currentTimeMillis();

    IncrementalIndex toPersist = IncrementalIndexTest.createCaseInsensitiveIndex(timestamp);

    final File tempDir = Files.createTempDir();
    try {
      QueryableIndex index = IndexIO.loadIndex(IndexMerger.persist(toPersist, tempDir));

      Assert.assertEquals(2, index.getTimeColumn().getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index.getAvailableDimensions()));
      Assert.assertEquals(2, index.getColumnNames().size());
    }
    finally {
      tempDir.delete();
    }
  }

  @Test
  public void testPersistMergeCaseInsensitive() throws Exception
  {
    final long timestamp = System.currentTimeMillis();
    IncrementalIndex toPersist1 = IncrementalIndexTest.createCaseInsensitiveIndex(timestamp);

    IncrementalIndex toPersist2 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("DIm1", "DIM2"),
            ImmutableMap.<String, Object>of("dim1", "1", "dim2", "2", "DIm1", "10000", "DIM2", "100000000")
        )
    );

    toPersist2.add(
        new MapBasedInputRow(
            timestamp,
            Arrays.asList("dIM1", "dIm2"),
            ImmutableMap.<String, Object>of("DIm1", "1", "DIM2", "2", "dim1", "5", "dim2", "6")
        )
    );

    final File tempDir1 = Files.createTempDir();
    final File tempDir2 = Files.createTempDir();
    final File mergedDir = Files.createTempDir();
    try {
      QueryableIndex index1 = IndexIO.loadIndex(IndexMerger.persist(toPersist1, tempDir1));

      Assert.assertEquals(2, index1.getTimeColumn().getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index1.getAvailableDimensions()));
      Assert.assertEquals(2, index1.getColumnNames().size());

      QueryableIndex index2 = IndexIO.loadIndex(IndexMerger.persist(toPersist2, tempDir2));

      Assert.assertEquals(2, index2.getTimeColumn().getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(index2.getAvailableDimensions()));
      Assert.assertEquals(2, index2.getColumnNames().size());

      QueryableIndex merged = IndexIO.loadIndex(
          IndexMerger.mergeQueryableIndex(Arrays.asList(index1, index2), new AggregatorFactory[]{}, mergedDir)
      );

      Assert.assertEquals(3, merged.getTimeColumn().getLength());
      Assert.assertEquals(Arrays.asList("dim1", "dim2"), Lists.newArrayList(merged.getAvailableDimensions()));
      Assert.assertEquals(2, merged.getColumnNames().size());
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
    final IncrementalIndex toPersist1 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});
    final IncrementalIndex toPersist2 = new IncrementalIndex(0L, QueryGranularity.NONE, new AggregatorFactory[]{});
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

      Assert.assertEquals(1, index1.getTimeColumn().getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index1.getAvailableDimensions()));

      Assert.assertEquals(1, index2.getTimeColumn().getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(index2.getAvailableDimensions()));

      Assert.assertEquals(1, merged.getTimeColumn().getLength());
      Assert.assertEquals(ImmutableList.of("dim2"), ImmutableList.copyOf(merged.getAvailableDimensions()));
    } finally {
      FileUtils.deleteQuietly(tmpDir1);
      FileUtils.deleteQuietly(tmpDir2);
      FileUtils.deleteQuietly(tmpDir3);
    }
  }
}
