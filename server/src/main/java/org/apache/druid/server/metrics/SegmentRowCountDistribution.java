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

import org.apache.druid.java.util.emitter.EmittingLogger;

import java.util.function.ObjIntConsumer;

/**
 * Class that creates a count of segments that have row counts in certain buckets
 */
public class SegmentRowCountDistribution
{
  private static final EmittingLogger log = new EmittingLogger(SegmentRowCountDistribution.class);

  private final int[] buckets = new int[9];
  private static final int TOMBSTONE_BUCKET_INDEX = 0;

  /**
   * Increments the count for a particular bucket held in this class
   *
   * @param rowCount the number of rows to figure out which bucket to increment
   */
  public void addRowCountToDistribution(long rowCount)
  {
    int bucketIndex = determineBucketFromRowCount(rowCount);
    buckets[bucketIndex]++;
  }

  /**
   * Decrements the count for a particular bucket held in this class
   *
   * @param rowCount the count which determines which bucket to decrement
   */
  public void removeRowCountFromDistribution(long rowCount)
  {
    int bucketIndex = determineBucketFromRowCount(rowCount);
    buckets[bucketIndex]--;
    if (buckets[bucketIndex] < 0) {
      // can this ever go negative?
      log.error("somehow got a count of less than 0, resetting value to 0");
      buckets[bucketIndex] = 0;
    }
  }

  /**
   * Increments the count for number of tombstones in the distribution
   */
  public void addTombstoneToDistribution()
  {
    buckets[TOMBSTONE_BUCKET_INDEX]++;
  }

  /**
   * Decrements the count for the number of tombstones in he distribution.
   */
  public void removeTombstoneFromDistribution()
  {
    buckets[TOMBSTONE_BUCKET_INDEX]--;
  }

  /**
   * Determines the name of the dimension used for a bucket. Should never return NA as this isn't public and this
   * method is private to this class
   *
   * @param index the index of the bucket
   * @return the dimension which the bucket index refers to
   */
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

  /**
   * Figures out which bucket the specified rowCount belongs to
   *
   * @param rowCount the number of rows in a segment
   * @return the bucket index
   */
  private static int determineBucketFromRowCount(long rowCount)
  {
    // 0 indexed bucket is reserved for tombstones
    if (rowCount <= 0L) {
      return 1;
    }
    if (rowCount <= 10_000L) {
      return 2;
    }
    if (rowCount <= 2_000_000L) {
      return 3;
    }
    if (rowCount <= 4_000_000L) {
      return 4;
    }
    if (rowCount <= 6_000_000L) {
      return 5;
    }
    if (rowCount <= 8_000_000L) {
      return 6;
    }
    if (rowCount <= 10_000_000L) {
      return 7;
    }
    return 8;
  }

  /**
   * Gives the consumer the range dimension and the associated count. Will not give zero range unless there is a count there.
   *
   * @param consumer
   */
  public void forEachDimension(final ObjIntConsumer<String> consumer)
  {
    for (int ii = 0; ii < buckets.length; ii++) {
      // only report tombstones and 0 bucket if it has nonzero value
      if (ii > 1 || buckets[ii] != 0) {
        consumer.accept(getBucketDimensionFromIndex(ii), buckets[ii]);
      }
    }
  }

}
