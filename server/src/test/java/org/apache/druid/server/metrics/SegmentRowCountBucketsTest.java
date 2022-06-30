package org.apache.druid.server.metrics;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.function.ObjIntConsumer;


public class SegmentRowCountBucketsTest
{

  private SegmentRowCountBuckets rowCountBucket;

  @Before
  public void setUp()
  {
    rowCountBucket = new SegmentRowCountBuckets();
  }

  @Test
  public void test_bucketCountSanity()
  {
    // test base case
     rowCountBucket.forEachDimension((final String dimension, final int count) -> {
       Assert.assertEquals(0, count);
     });


    // test bounds of 1st bucket
    // with addition
    rowCountBucket.addRowCountToBucket(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 0));
    rowCountBucket.addRowCountToBucket(100_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 0));
    // with removal
    rowCountBucket.removeRowCountfromBucket(0);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 0));
    rowCountBucket.removeRowCountfromBucket(100_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 0));

    // test bounds of 2nd bucket
    // with addition
    rowCountBucket.addRowCountToBucket(100_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 1));
    rowCountBucket.addRowCountToBucket(1_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 1));
    // with removal
    rowCountBucket.removeRowCountfromBucket(100_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 1));
    rowCountBucket.removeRowCountfromBucket(1_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 1));

    // test bounds of 3rd bucket
    // with addition
    rowCountBucket.addRowCountToBucket(1_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 2));
    rowCountBucket.addRowCountToBucket(2_000_000);
    // with removal
    rowCountBucket.removeRowCountfromBucket(1_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 2));
    rowCountBucket.removeRowCountfromBucket(2_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 2));

    // test bounds of 4th bucket
    // with addition
    rowCountBucket.addRowCountToBucket(2_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 3));

    rowCountBucket.addRowCountToBucket(3_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 3));

    // with removal
    rowCountBucket.removeRowCountfromBucket(2_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 3));

    rowCountBucket.removeRowCountfromBucket(3_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 3));


    // test bounds of 5th bucket
    // with addition
    rowCountBucket.addRowCountToBucket(3_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 4));
    rowCountBucket.addRowCountToBucket(4_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 4));
    // with removal
    rowCountBucket.removeRowCountfromBucket(3_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 4));
    rowCountBucket.removeRowCountfromBucket(4_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 4));

    // test bounds of 6th bucket
    // with addition
    rowCountBucket.addRowCountToBucket(4_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 5));
    rowCountBucket.addRowCountToBucket(6_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 5));
    // with removal
    rowCountBucket.removeRowCountfromBucket(4_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 5));
    rowCountBucket.removeRowCountfromBucket(6_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 5));

    // test bounds of 7th bucket
    // with addition
    rowCountBucket.addRowCountToBucket(6_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 6));
    rowCountBucket.addRowCountToBucket(7_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 6));
    // with removal
    rowCountBucket.removeRowCountfromBucket(6_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 6));
    rowCountBucket.removeRowCountfromBucket(7_000_000);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 6));

    // test bounds of 8th bucket
    // with addition
    rowCountBucket.addRowCountToBucket(7_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 7));
    rowCountBucket.addRowCountToBucket(Long.MAX_VALUE);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(2, 7));
    // with removal
    rowCountBucket.removeRowCountfromBucket(7_000_001);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(1, 7));
    rowCountBucket.removeRowCountfromBucket(Long.MAX_VALUE);
    rowCountBucket.forEachDimension(AssertBucketHasValue.assertExpected(0, 7));
  }

  @Test
  public void test_bucketDimensionFromIndex()
  {
    Assert.assertEquals("0-100k", SegmentRowCountBuckets.getBucketDimensionFromIndex(0));
    Assert.assertEquals("100k-1M", SegmentRowCountBuckets.getBucketDimensionFromIndex(1));
    Assert.assertEquals("1M-2M", SegmentRowCountBuckets.getBucketDimensionFromIndex(2));
    Assert.assertEquals("2M-3M", SegmentRowCountBuckets.getBucketDimensionFromIndex(3));
    Assert.assertEquals("3M-4M", SegmentRowCountBuckets.getBucketDimensionFromIndex(4));
    Assert.assertEquals("4M-6M", SegmentRowCountBuckets.getBucketDimensionFromIndex(5));
    Assert.assertEquals("6M-7M", SegmentRowCountBuckets.getBucketDimensionFromIndex(6));
    Assert.assertEquals("7M+", SegmentRowCountBuckets.getBucketDimensionFromIndex(7));
    Assert.assertEquals("NA", SegmentRowCountBuckets.getBucketDimensionFromIndex(8));
  }

  private static class AssertBucketHasValue implements ObjIntConsumer<String> {

    final int expectedBucket;
    final int expectedValue;
    private AssertBucketHasValue(int expectedBucket, int expectedValue) {
      this.expectedBucket = expectedBucket;
      this.expectedValue = expectedValue;
    }

    static AssertBucketHasValue assertExpected(int expectedValue, int expectedBucket){
      return new AssertBucketHasValue(expectedBucket, expectedValue);
    }

    @Override
    public void accept(String s, int value)
    {
      if (s.equals(SegmentRowCountBuckets.getBucketDimensionFromIndex(expectedBucket))) {
        Assert.assertEquals(expectedValue, value);
      } else {
        // assert all other values are empty
        Assert.assertEquals(0, value);
      }
    }
  }

}
