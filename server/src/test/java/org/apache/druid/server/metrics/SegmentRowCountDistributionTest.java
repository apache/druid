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

package org.apache.druid.server.metrics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.ObjIntConsumer;


public class SegmentRowCountDistributionTest
{

  private SegmentRowCountDistribution rowCountBucket;

  @Before
  public void setUp()
  {
    rowCountBucket = new SegmentRowCountDistribution();
  }

  @Test
  public void test_bucketCountSanity()
  {
    // test base case
    rowCountBucket.forEachDimension((final String dimension, final int count) -> {
      Assert.assertEquals(0, count);
    });

    // tombstones
    // add tombstones
    rowCountBucket.addTombstoneToDistribution();
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 0));
    rowCountBucket.addTombstoneToDistribution();
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 0));
    // remove tombstones
    rowCountBucket.removeTombstoneFromDistribution();
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 0));
    rowCountBucket.removeTombstoneFromDistribution();
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 0));

    // test bounds of 1st bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 1));
    rowCountBucket.addRowCountToDistribution(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 1));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 1));
    rowCountBucket.removeRowCountFromDistribution(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 1));

    // test bounds of 2nd bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(1);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 2));
    rowCountBucket.addRowCountToDistribution(10_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 2));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(1);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 2));
    rowCountBucket.removeRowCountFromDistribution(10_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 2));

    // test bounds of 3rd bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(10_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 3));
    rowCountBucket.addRowCountToDistribution(2_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 3));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(10_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 3));
    rowCountBucket.removeRowCountFromDistribution(2_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 3));

    // test bounds of 4th bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(2_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 4));
    rowCountBucket.addRowCountToDistribution(4_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 4));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(2_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 4));

    rowCountBucket.removeRowCountFromDistribution(4_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 4));


    // test bounds of 5th bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(4_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 5));
    rowCountBucket.addRowCountToDistribution(6_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 5));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(4_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 5));
    rowCountBucket.removeRowCountFromDistribution(6_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 5));

    // test bounds of 6th bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(6_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 6));
    rowCountBucket.addRowCountToDistribution(8_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 6));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(6_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 6));
    rowCountBucket.removeRowCountFromDistribution(8_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 6));

    // test bounds of 7th bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(8_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 7));
    rowCountBucket.addRowCountToDistribution(10_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 7));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(8_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 7));
    rowCountBucket.removeRowCountFromDistribution(10_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 7));

    // test bounds of 8th bucket
    // with addition
    rowCountBucket.addRowCountToDistribution(10_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 8));
    rowCountBucket.addRowCountToDistribution(Long.MAX_VALUE);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 8));
    // with removal
    rowCountBucket.removeRowCountFromDistribution(10_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 8));
    rowCountBucket.removeRowCountFromDistribution(Long.MAX_VALUE);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 8));
  }

  // this is used to test part of the functionality in AssertBucketHasValue.assertExpected
  @Test
  public void test_bucketDimensionFromIndex()
  {
    Assert.assertEquals("Tombstone", getBucketDimensionFromIndex(0));
    Assert.assertEquals("0", getBucketDimensionFromIndex(1));
    Assert.assertEquals("1-10k", getBucketDimensionFromIndex(2));
    Assert.assertEquals("10k-2M", getBucketDimensionFromIndex(3));
    Assert.assertEquals("2M-4M", getBucketDimensionFromIndex(4));
    Assert.assertEquals("4M-6M", getBucketDimensionFromIndex(5));
    Assert.assertEquals("6M-8M", getBucketDimensionFromIndex(6));
    Assert.assertEquals("8M-10M", getBucketDimensionFromIndex(7));
    Assert.assertEquals("10M+", getBucketDimensionFromIndex(8));
    Assert.assertEquals("NA", getBucketDimensionFromIndex(9));
  }

  private static class AssertBucketHasValue implements ObjIntConsumer<String>
  {

    private final int expectedBucket;
    private final int expectedValue;

    private AssertBucketHasValue(int expectedBucket, int expectedValue)
    {
      this.expectedBucket = expectedBucket;
      this.expectedValue = expectedValue;
    }

    static AssertBucketHasValue assertExpected(int expectedValue, int expectedBucket)
    {
      return new AssertBucketHasValue(expectedBucket, expectedValue);
    }

    @Override
    public void accept(String s, int value)
    {
      if (s.equals(getBucketDimensionFromIndex(expectedBucket))) {
        Assert.assertEquals(expectedValue, value);
      } else {
        // assert all other values are empty
        Assert.assertEquals(0, value);
      }
    }
  }

  // this is here because we didn't want to expose the internals of the buckets for segment rowCount distributions
  private static String getBucketDimensionFromIndex(int index)
  {
    switch (index) {
      case 0:
        return "Tombstone";
      case 1:
        return "0";
      case 2:
        return "1-10k";
      case 3:
        return "10k-2M";
      case 4:
        return "2M-4M";
      case 5:
        return "4M-6M";
      case 6:
        return "6M-8M";
      case 7:
        return "8M-10M";
      case 8:
        return "10M+";
      // should never get to default
      default:
        return "NA";
    }
  }


}
