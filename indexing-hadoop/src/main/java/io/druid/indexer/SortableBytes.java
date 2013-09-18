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

package io.druid.indexer;

import com.google.common.base.Charsets;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Partitioner;

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

    if ("".equals(sortKey)) {
      throw new IllegalArgumentException();
    }
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
           "groupKey='" + new String(groupKey, Charsets.UTF_8) + '\'' +
           ", sortKey='" + new String(sortKey, Charsets.UTF_8) + '\'' +
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
