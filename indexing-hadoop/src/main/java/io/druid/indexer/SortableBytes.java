/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.indexer;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

import io.druid.java.util.common.StringUtils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 */
public class SortableBytes
{
  private final byte[] groupKey;
  private final byte[] sortKey;

  public SortableBytes(
      byte[] groupKey,
      byte[] sortKey
  )
  {
    this.groupKey = groupKey;
    this.sortKey = sortKey;
  }

  public byte[] getGroupKey()
  {
    return groupKey;
  }

  public byte[] getSortKey()
  {
    return sortKey;
  }

  public byte[] toBytes()
  {
    ByteBuffer outBytes = ByteBuffer.wrap(new byte[4 + groupKey.length + sortKey.length]);
    outBytes.putInt(groupKey.length);
    outBytes.put(groupKey);
    outBytes.put(sortKey);
    return outBytes.array();
  }

  public BytesWritable toBytesWritable()
  {
    return new BytesWritable(toBytes());
  }

  @Override
  public String toString()
  {
    return "SortableBytes{" +
           "groupKey='" + StringUtils.fromUtf8(groupKey) + '\'' +
           ", sortKey='" + StringUtils.fromUtf8(sortKey) + '\'' +
           '}';
  }

  public static SortableBytes fromBytes(byte[] bytes)
  {
    return fromBytes(bytes, 0, bytes.length);
  }

  public static SortableBytes fromBytesWritable(BytesWritable bytes)
  {
    return fromBytes(bytes.getBytes(), 0, bytes.getLength());
  }

  public static SortableBytes fromBytes(byte[] bytes, int offset, int length)
  {
    int groupKeySize = ByteBuffer.wrap(bytes, offset, length).getInt();

    int sortKeyOffset = offset + 4 + groupKeySize;
    return new SortableBytes(
        Arrays.copyOfRange(bytes, offset + 4, sortKeyOffset),
        Arrays.copyOfRange(bytes, sortKeyOffset, offset + length)
    );
  }

  public static void useSortableBytesAsMapOutputKey(Job job)
  {
    job.setMapOutputKeyClass(BytesWritable.class);
    job.setGroupingComparatorClass(SortableBytesGroupingComparator.class);
    job.setSortComparatorClass(SortableBytesSortingComparator.class);
    job.setPartitionerClass(SortableBytesPartitioner.class);
  }

  public static class SortableBytesGroupingComparator extends WritableComparator
  {

    protected SortableBytesGroupingComparator()
    {
      super(BytesWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
    {
      int b1Length = ByteBuffer.wrap(b1, s1 + 4, l1 - 4).getInt();
      int b2Length = ByteBuffer.wrap(b2, s2 + 4, l2 - 4).getInt();

      final int retVal = compareBytes(
          b1, s1 + 8, b1Length,
          b2, s2 + 8, b2Length
      );

      return retVal;
    }
  }

  public static class SortableBytesSortingComparator extends WritableComparator
  {

    protected SortableBytesSortingComparator()
    {
      super(BytesWritable.class);
    }

    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2)
    {
      int b1Length = ByteBuffer.wrap(b1, s1 + 4, l1 - 4).getInt();
      int b2Length = ByteBuffer.wrap(b2, s2 + 4, l2 - 4).getInt();

      int retVal = compareBytes(
          b1, s1 + 8, b1Length,
          b2, s2 + 8, b2Length
      );

      if (retVal == 0) {
        retVal = compareBytes(
            b1, s1 + 8 + b1Length, l1 - 8 - b1Length,
            b2, s2 + 8 + b2Length, l2 - 8 - b2Length
        );
      }

      return retVal;
    }
  }

  public static class SortableBytesPartitioner extends Partitioner<BytesWritable, Object>
  {
    @Override
    public int getPartition(BytesWritable bytesWritable, Object o, int numPartitions)
    {
      final byte[] bytes = bytesWritable.getBytes();
      int length = ByteBuffer.wrap(bytes).getInt();

      return (ByteBuffer.wrap(bytes, 4, length).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
  }
}
