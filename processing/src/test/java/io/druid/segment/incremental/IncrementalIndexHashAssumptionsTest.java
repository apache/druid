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

package io.druid.segment.incremental;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Striped;
import org.joda.time.DateTime;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.locks.Lock;

/**
 * This class tests a few key assumptions that IncrementalIndex makes about how hashes are handled
 */
@RunWith(Parameterized.class)
public class IncrementalIndexHashAssumptionsTest
{
  @Parameterized.Parameters
  public static Collection<Object[]> getParameters(){
    return ImmutableList.<Object[]>of(
        new Object[]{1, new Integer[]{4,5,6,7,8,9,10,9,8,7,6,5,4,4,4,4}},
        new Object[]{1000, new Integer[]{4,7,4,4,7,7,9,8,8,8,7,6,6,7,5,3}},
        new Object[]{60 * 1000, new Integer[]{4,6,9,4,5,12,8,5,3,3,8,8,5,9,7,4}},
        new Object[]{60 * 60 * 1000, new Integer[]{9,4,8,10,5,8,5,7,4,3,4,5,3,9,9,7}},
        new Object[]{24 * 60 * 60 * 1000, new Integer[]{8,5,6,6,3,7,8,5,10,8,8,4,3,5,7,7}},
        new Object[]{7 * 24 * 60 * 60 * 1000, new Integer[]{5,10,5,6,6,10,6,2,7,6,7,11,3,1,7,8}}
        );
  }
  public IncrementalIndexHashAssumptionsTest(long stepSize, Integer[] expectedResults){
    this.stepSize =stepSize;
    this.expectedResults = expectedResults;
  }
  private final long stepSize;
  private final Integer[] expectedResults;
  private final int stripeSize = 16;
  private final Striped<Lock> stripedLock = Striped.lazyWeakLock(stripeSize);
  private final String[] defaultDimNames = new String[]{"dim1","dim2","dim3"};
  private final String[][] defaultDimValues = new String[][]{
      new String[]{"1"},
      new String[]{"2"},
      new String[]{"3"}
  };
  @Test
  public void testHashing(){
    final long start = DateTime.parse("2015-01-01T00:00:00Z").getMillis();
    final Integer[] hashBuckets = new Integer[stripeSize];
    for(int i = 0; i < stripeSize; ++i){
      hashBuckets[i] = 0;
    }
    for(long i = 0; i < 100; ++i){
      final IncrementalIndex.TimeAndDims timeAndDims = new IncrementalIndex.TimeAndDims(start + i*stepSize, defaultDimValues);
      final int hash = timeAndDims.hashCode();
      final int bucket = Math.abs(hash) % hashBuckets.length;
      ++hashBuckets[bucket];
    }
    Assert.assertArrayEquals(expectedResults, hashBuckets);
  }
}
