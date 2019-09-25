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

package org.apache.druid.query.aggregation;

import org.apache.druid.query.PerSegmentQueryOptimizationContext;
import org.apache.druid.query.cache.CacheKeyBuilder;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.vector.VectorColumnSelectorFactory;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

/**
 * This AggregatorFactory is meant for wrapping delegate aggregators for optimization purposes.
 *
 * The wrapper suppresses the aggregate() method for the underlying delegate, while leaving
 * the behavior of other calls unchanged.
 *
 * This wrapper is meant to be used when an optimization decides that an aggregator can be entirely skipped
 * (e.g., a FilteredAggregatorFactory where the filter condition will never match).
 */
public class SuppressedAggregatorFactory extends AggregatorFactory
{
  private final AggregatorFactory delegate;

  public SuppressedAggregatorFactory(
      AggregatorFactory delegate
  )
  {
    this.delegate = delegate;
  }

  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    return new SuppressedAggregator(delegate.factorize(metricFactory));
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    return new SuppressedBufferAggregator(delegate.factorizeBuffered(metricFactory));
  }

  @Override
  public VectorAggregator factorizeVector(VectorColumnSelectorFactory columnSelectorFactory)
  {
    return new SuppressedVectorAggregator(delegate.factorizeVector(columnSelectorFactory));
  }

  @Override
  public boolean canVectorize()
  {
    return delegate.canVectorize();
  }

  @Override
  public Comparator getComparator()
  {
    return delegate.getComparator();
  }

  @Override
  public Object combine(Object lhs, Object rhs)
  {
    return delegate.combine(lhs, rhs);
  }

  @Override
  public AggregateCombiner makeAggregateCombiner()
  {
    return delegate.makeAggregateCombiner();
  }

  @Override
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
  }

  @Override
  public AggregatorFactory getMergingFactory(AggregatorFactory other) throws AggregatorFactoryNotMergeableException
  {
    return delegate.getMergingFactory(other);
  }

  @Override
  public List<AggregatorFactory> getRequiredColumns()
  {
    return delegate.getRequiredColumns();
  }

  @Override
  public Object deserialize(Object object)
  {
    return delegate.deserialize(object);
  }

  @Nullable
  @Override
  public Object finalizeComputation(@Nullable Object object)
  {
    return delegate.finalizeComputation(object);
  }

  @Override
  public String getName()
  {
    return delegate.getName();
  }

  @Override
  public List<String> requiredFields()
  {
    return delegate.requiredFields();
  }

  @Override
  public String getTypeName()
  {
    return delegate.getTypeName();
  }

  @Override
  public int getMaxIntermediateSize()
  {
    return delegate.getMaxIntermediateSize();
  }

  @Override
  public int getMaxIntermediateSizeWithNulls()
  {
    return delegate.getMaxIntermediateSizeWithNulls();
  }

  @Override
  public AggregatorFactory optimizeForSegment(PerSegmentQueryOptimizationContext optimizationContext)
  {
    // we are already the result of an optimizeForSegment() call
    return this;
  }

  @Override
  public byte[] getCacheKey()
  {
    CacheKeyBuilder cacheKeyBuilder = new CacheKeyBuilder(AggregatorUtil.SUPPRESSED_AGG_CACHE_TYPE_ID);
    cacheKeyBuilder.appendCacheable(delegate);
    return cacheKeyBuilder.build();
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
    SuppressedAggregatorFactory that = (SuppressedAggregatorFactory) o;
    return Objects.equals(getDelegate(), that.getDelegate());
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(getDelegate());
  }

  @Override
  public String toString()
  {
    return "SuppressedAggregatorFactory{" +
           "delegate=" + delegate +
           '}';
  }

  public AggregatorFactory getDelegate()
  {
    return delegate;
  }

  public static class SuppressedAggregator implements Aggregator
  {
    private final Aggregator delegate;

    public SuppressedAggregator(
        Aggregator delegate
    )
    {
      this.delegate = delegate;
    }

    @Override
    public void aggregate()
    {
      //no-op
    }

    @Nullable
    @Override
    public Object get()
    {
      return delegate.get();
    }

    @Override
    public float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public long getLong()
    {
      return delegate.getLong();
    }

    @Override
    public double getDouble()
    {
      return delegate.getDouble();
    }

    @Override
    public boolean isNull()
    {
      return delegate.isNull();
    }

    @Override
    public void close()
    {
      delegate.close();
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
      SuppressedAggregator that = (SuppressedAggregator) o;
      return Objects.equals(getDelegate(), that.getDelegate());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getDelegate());
    }

    @Override
    public String toString()
    {
      return "SuppressedAggregator{" +
             "delegate=" + delegate +
             '}';
    }

    public Aggregator getDelegate()
    {
      return delegate;
    }
  }

  public static class SuppressedBufferAggregator implements BufferAggregator
  {
    private final BufferAggregator delegate;

    public SuppressedBufferAggregator(
        BufferAggregator delegate
    )
    {
      this.delegate = delegate;
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      delegate.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position)
    {
      // no-op
    }

    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return delegate.get(buf, position);
    }

    @Override
    public float getFloat(ByteBuffer buf, int position)
    {
      return delegate.getFloat(buf, position);
    }

    @Override
    public long getLong(ByteBuffer buf, int position)
    {
      return delegate.getLong(buf, position);
    }

    @Override
    public double getDouble(ByteBuffer buf, int position)
    {
      return delegate.getDouble(buf, position);
    }

    @Override
    public void close()
    {
      delegate.close();
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
      delegate.inspectRuntimeShape(inspector);
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
    {
      delegate.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public boolean isNull(ByteBuffer buf, int position)
    {
      return delegate.isNull(buf, position);
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
      SuppressedBufferAggregator that = (SuppressedBufferAggregator) o;
      return Objects.equals(getDelegate(), that.getDelegate());
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(getDelegate());
    }

    @Override
    public String toString()
    {
      return "SuppressedBufferAggregator{" +
             "delegate=" + delegate +
             '}';
    }

    public BufferAggregator getDelegate()
    {
      return delegate;
    }
  }

  public static class SuppressedVectorAggregator implements VectorAggregator
  {
    private final VectorAggregator delegate;

    public SuppressedVectorAggregator(VectorAggregator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public void init(ByteBuffer buf, int position)
    {
      delegate.init(buf, position);
    }

    @Override
    public void aggregate(ByteBuffer buf, int position, int startRow, int endRow)
    {
      // no-op
    }

    @Override
    public void aggregate(ByteBuffer buf, int numRows, int[] positions, @Nullable int[] rows, int positionOffset)
    {
      // no-op
    }

    @Nullable
    @Override
    public Object get(ByteBuffer buf, int position)
    {
      return delegate.get(buf, position);
    }

    @Override
    public void relocate(int oldPosition, int newPosition, ByteBuffer oldBuffer, ByteBuffer newBuffer)
    {
      delegate.relocate(oldPosition, newPosition, oldBuffer, newBuffer);
    }

    @Override
    public void close()
    {
      delegate.close();
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
      SuppressedVectorAggregator that = (SuppressedVectorAggregator) o;
      return Objects.equals(delegate, that.delegate);
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(delegate);
    }

    @Override
    public String toString()
    {
      return "SuppressedVectorAggregator{" +
             "delegate=" + delegate +
             '}';
    }
  }
}
