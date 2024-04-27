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

package org.apache.druid.query.aggregation.variance;

import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import org.apache.druid.common.config.NullHandling;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 *
 * Algorithm used here is copied from apache hive. This is description in GenericUDAFVariance
 *
 * Evaluate the variance using the algorithm described by Chan, Golub, and LeVeque in
 * "Algorithms for computing the sample variance: analysis and recommendations"
 * The American Statistician, 37 (1983) pp. 242--247.
 *
 * variance = variance1 + variance2 + n/(m*(m+n)) * pow(((m/n)*t1 - t2),2)
 *
 * where: - variance is sum[x-avg^2] (this is actually n times the variance)
 * and is updated at every step. - n is the count of elements in chunk1 - m is
 * the count of elements in chunk2 - t1 = sum of elements in chunk1, t2 =
 * sum of elements in chunk2.
 *
 * This algorithm was proven to be numerically stable by J.L. Barlow in
 * "Error analysis of a pairwise summation algorithm to compute sample variance"
 * Numer. Math, 58 (1991) pp. 583--590
 */
public class VarianceAggregatorCollector
{
  public static boolean isVariancePop(@Nullable String estimator)
  {
    return estimator != null && "population".equalsIgnoreCase(estimator);
  }

  public static VarianceAggregatorCollector from(ByteBuffer buffer)
  {
    return new VarianceAggregatorCollector(buffer.getLong(), buffer.getDouble(), buffer.getDouble());
  }

  public static final Comparator<VarianceAggregatorCollector> COMPARATOR = (o1, o2) -> {
    int compare = Doubles.compare(o1.nvariance, o2.nvariance);
    if (compare == 0) {
      compare = Longs.compare(o1.count, o2.count);
      if (compare == 0) {
        compare = Doubles.compare(o1.sum, o2.sum);
      }
    }
    return compare;
  };

  void fold(@Nullable VarianceAggregatorCollector other)
  {
    if (other == null || other.count == 0) {
      return;
    }

    if (this.count == 0) {
      this.nvariance = other.nvariance;
      this.count = other.count;
      this.sum = other.sum;
      return;
    }
    final double ratio = this.count / (double) other.count;
    final double t = this.sum / ratio - other.sum;

    this.nvariance += other.nvariance + (ratio / (this.count + other.count) * t * t);
    this.count += other.count;
    this.sum += other.sum;
  }

  static Object combineValues(@Nullable Object lhs, @Nullable Object rhs)
  {
    if (lhs == null) {
      return rhs;
    }
    ((VarianceAggregatorCollector) lhs).fold((VarianceAggregatorCollector) rhs);
    return lhs;
  }

  static int getMaxIntermediateSize()
  {
    return Long.BYTES + Double.BYTES + Double.BYTES;
  }

  long count; // number of elements
  double sum; // sum of elements
  double nvariance; // sum[x-avg^2] (this is actually n times of the variance)

  public VarianceAggregatorCollector()
  {
    this(0, 0, 0);
  }

  void copyFrom(VarianceAggregatorCollector other)
  {
    this.count = other.count;
    this.sum = other.sum;
    this.nvariance = other.nvariance;
  }

  public VarianceAggregatorCollector(long count, double sum, double nvariance)
  {
    this.count = count;
    this.sum = sum;
    this.nvariance = nvariance;
  }

  public VarianceAggregatorCollector add(float v)
  {
    count++;
    sum += v;
    if (count > 1) {
      double t = count * v - sum;
      nvariance += (t * t) / ((double) count * (count - 1));
    }
    return this;
  }

  public VarianceAggregatorCollector add(double v)
  {
    count++;
    sum += v;
    if (count > 1) {
      double t = count * v - sum;
      nvariance += (t * t) / ((double) count * (count - 1));
    }
    return this;
  }

  public VarianceAggregatorCollector add(long v)
  {
    count++;
    sum += v;
    if (count > 1) {
      double t = count * v - sum;
      nvariance += (t * t) / ((double) count * (count - 1));
    }
    return this;
  }

  @Nullable
  public Double getVariance(boolean variancePop)
  {
    if (count == 0) {
      return NullHandling.defaultDoubleValue();
    } else if (count == 1) {
      return 0d;
    } else {
      return variancePop ? nvariance / count : nvariance / (count - 1);
    }
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
    return ByteBuffer.allocate(Long.BYTES + Double.BYTES + Double.BYTES)
                     .putLong(count)
                     .putDouble(sum)
                     .putDouble(nvariance);
  }

  @VisibleForTesting
  boolean equalsWithEpsilon(VarianceAggregatorCollector o, double epsilon)
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
    if (Math.abs(nvariance - o.nvariance) > epsilon) {
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

    VarianceAggregatorCollector that = (VarianceAggregatorCollector) o;

    if (count != that.count) {
      return false;
    }
    if (Double.compare(that.sum, sum) != 0) {
      return false;
    }
    if (Double.compare(that.nvariance, nvariance) != 0) {
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
    temp = Double.doubleToLongBits(nvariance);
    result = 31 * result + (int) (temp ^ (temp >>> 32));
    return result;
  }

  @Override
  public String toString()
  {
    return "VarianceHolder{" +
           "count=" + count +
           ", sum=" + sum +
           ", nvariance=" + nvariance +
           '}';
  }
}
