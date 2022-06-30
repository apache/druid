package org.apache.druid.server.metrics;

import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.function.ObjIntConsumer;

/**
 * Class that creates a count of segments that have row counts in certain buckets
 */
public class SegmentRowCountBuckets
{
  private static final EmittingLogger log = new EmittingLogger(SegmentRowCountBuckets.class);

  private final int[] buckets = new int[8];

  /**
   * Increments the count for a particular bucket held in this class
   * @param rowCount the number of rows to figure out which bucket to increment
   */
  public void addRowCountToBucket(long rowCount)
  {
    int bucketIndex = determineBucket(rowCount);
    buckets[bucketIndex]++;
  }

  /**
   * Decrements the count for a particular bucket held in this class
   * @param rowCount the count which determines which bucket to decrement
   */
  public void removeRowCountfromBucket(long rowCount)
  {
    int bucketIndex = determineBucket(rowCount);
    buckets[bucketIndex]--;
    if (buckets[bucketIndex] < 0) {
      // can this ever go negative?
      log.error("somehow got a count of less than 0, resetting value to 0");
      buckets[bucketIndex] = 0;
    }
  }

  /**
   * Determines the name of the dimension used for a bucket
   * @param index the index of the bucket
   * @return the dimension which the bucket index refers to
   */
  static String getBucketDimensionFromIndex(int index)
  {
    switch (index) {
      case 0:
        return "0-100k";
      case 1:
        return "100k-1M";
      case 2:
        return "1M-2M";
      case 3:
        return "2M-3M";
      case 4:
        return "3M-4M";
      case 5:
        return "4M-6M";
      case 6:
        return "6M-7M";
      case 7:
        return "7M+";
      // should never get to default
      default:
        return "NA";
    }
  }

  /**
   * Figures out which bucket the specified rowCount belongs to
   * @param rowCount the number of rows in a segment
   * @return the bucket index
   */
  private static int determineBucket(long rowCount)
  {
    if (rowCount <= 100_000L) {
      return 0;
    }
    if (rowCount <= 1_000_000L) {
      return 1;
    }
    if (rowCount <= 2_000_000L) {
      return 2;
    }
    if (rowCount <= 3_000_000L) {
      return 3;
    }
    if (rowCount <= 4_000_000L) {
      return 4;
    }
    if (rowCount <= 6_000_000L) {
      return 5;
    }
    if (rowCount <= 7_000_000L) {
      return 6;
    }
    return 7;
  }

  public void forEachDimension(final ObjIntConsumer<String> consumer)
  {
      for (int ii =0; ii < buckets.length; ii++ ) {
        consumer.accept(getBucketDimensionFromIndex(ii), buckets[ii]);
      }
  }

}
