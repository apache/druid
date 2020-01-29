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

package org.apache.druid.indexer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Pair;
import org.joda.time.DateTime;

import java.nio.ByteBuffer;

public class Bucket
{
  public static final int PREAMBLE_BYTES = 16;

  /** ID for this bucket, unique for this indexer run. Used for grouping and partitioning. */
  private final int shardNum;

  /** Start time of this bucket's time window. End time can be determined by our GranularitySpec. */
  public final DateTime time;

  /** Partition number of this bucket within our time window (other Buckets may occupy the same window). */
  public final int partitionNum;

  public Bucket(int shardNum, DateTime time, int partitionNum)
  {
    this.shardNum = shardNum;
    this.time = time;
    this.partitionNum = partitionNum;
  }

  public byte[] toGroupKey(byte[]... parts)
  {
    ByteBuffer buf = ByteBuffer.allocate(PREAMBLE_BYTES + sizes(parts));

    buf.putInt(shardNum);
    buf.putLong(time.getMillis());
    buf.putInt(partitionNum);
    for (byte[] part : parts) {
      buf.put(part);
    }

    return buf.array();
  }

  @Override
  public String toString()
  {
    return "Bucket{" +
           "time=" + time +
           ", partitionNum=" + partitionNum +
           ", shardNum=" + shardNum +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    Bucket bucket = (Bucket) o;

    if (partitionNum != bucket.partitionNum) {
      return false;
    }
    if (shardNum != bucket.shardNum) {
      return false;
    }
    if (time != null ? !time.equals(bucket.time) : bucket.time != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result = time != null ? time.hashCode() : 0;
    result = 31 * result + partitionNum;
    result = 31 * result + shardNum;
    return result;
  }

  private static int sizes(byte[]... parts)
  {
    int size = 0;
    for (byte[] part : parts) {
      size += part.length;
    }
    return size;
  }

  public static Pair<Bucket, byte[]> fromGroupKey(byte[] keyBytes)
  {
    ByteBuffer buf = ByteBuffer.wrap(keyBytes);

    Bucket bucket = new Bucket(buf.getInt(), DateTimes.utc(buf.getLong()), buf.getInt());
    byte[] bytesLeft = new byte[buf.remaining()];
    buf.get(bytesLeft);

    return Pair.of(bucket, bytesLeft);    
  }

  @VisibleForTesting
  protected int getShardNum()
  {
    return shardNum;
  }
}
