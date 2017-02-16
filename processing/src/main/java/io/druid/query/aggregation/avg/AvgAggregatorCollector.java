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

package io.druid.query.aggregation.avg;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class AvgAggregatorCollector
{
  public static AvgAggregatorCollector from(ByteBuffer buffer)
  {
    return new AvgAggregatorCollector(buffer.getLong(), buffer.getDouble());
  }

  public static final Comparator<AvgAggregatorCollector> COMPARATOR = new Comparator<AvgAggregatorCollector>()
  {
    @Override
    public int compare(AvgAggregatorCollector o1, AvgAggregatorCollector o2)
    {
      int compare = Longs.compare(o1.count, o2.count);
      if (compare != 0) {
        return compare;
      }
      return Doubles.compare(o1.sum, o2.sum);
    }
  };

  static Object combineValues(Object lhs, Object rhs)
  {
    final AvgAggregatorCollector holder1 = (AvgAggregatorCollector) lhs;
    final AvgAggregatorCollector holder2 = (AvgAggregatorCollector) rhs;

    if (holder2.count == 0) {
      return holder1;
    }
    if (holder1.count == 0) {
      holder1.count = holder2.count;
      holder1.sum = holder2.sum;
      return holder1;
    }

    holder1.count += holder2.count;
    holder1.sum += holder2.sum;

    return holder1;
  }

  static int getMaxIntermediateSize()
  {
    return Longs.BYTES + Doubles.BYTES;
  }

  long count; // number of elements
  double sum; // sum of elements

  public AvgAggregatorCollector()
  {
    this(0, 0);
  }

  public void reset()
  {
    count = 0;
    sum = 0;
  }

  public AvgAggregatorCollector(long count, double sum)
  {
    this.count = count;
    this.sum = sum;
  }

  public AvgAggregatorCollector add(float v)
  {
    count++;
    sum += v;
    return this;
  }

  public AvgAggregatorCollector add(long v)
  {
    count++;
    sum += v;
    return this;
  }

  public double compute()
  {
    if (count == 0) {
      throw new IllegalStateException("should not be empty holder");
    }
    return sum / count;
  }

  @JsonValue
  public byte[] toByteArray()
  {
    final ByteBuffer buffer = toByteBuffer();
    buffer.flip();
    byte[] theBytes = new byte[buffer.remaining()];
    buffer.get(theBytes);

    return theBytes;
  }

  public ByteBuffer toByteBuffer()
  {
    return ByteBuffer.allocate(Longs.BYTES + Doubles.BYTES)
                     .putLong(count)
                     .putDouble(sum);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(AvgAggregatorCollector o, double epsilon)
  {
    if (this == o) {
      return true;
    }

    if (count != o.count) {
      return false;
    }
    if (Math.abs(sum - o.sum) > epsilon) {
      return false;
    }

    return true;
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

    AvgAggregatorCollector that = (AvgAggregatorCollector) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(that.sum, sum) != 0) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode()
  {
    int result;
    long temp;
    result = (int) (count ^ (count >>> 32));
    temp = Double.doubleToLongBits(sum);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "AvgAggregatorCollector{" +
           "count=" + count +
           ", sum=" + sum +
           '}';
  }
}
