package io.druid.segment.data;

import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.BufferAggregator;
import io.druid.segment.ColumnSelectorFactory;
import org.junit.Assert;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;

/**
 * An AggregatorFactory that asserts thread safety.
 * If the delegate aggregator factory is accessed in a thread unsafe manner throws AssertionError during read.
 */
public class ThreadSafetyAssertingAggregatorFactory extends AggregatorFactory
{
  private final AggregatorFactory delegate;

  public ThreadSafetyAssertingAggregatorFactory(AggregatorFactory delegate)
  {
    this.delegate = delegate;
  }


  @Override
  public Aggregator factorize(ColumnSelectorFactory metricFactory)
  {
    final Aggregator delegate1 = delegate.factorize(metricFactory);
    final Aggregator delegate2 = delegate.factorize(metricFactory);
    return new Aggregator()
    {
      @Override
      public void aggregate()
      {
        delegate1.aggregate();
        Thread.yield();
        delegate2.aggregate();
      }

      @Override
      public void reset()
      {
        delegate1.reset();
        Thread.yield();
        delegate2.reset();
      }

      @Override
      public Object get()
      {
        Object o1 = delegate1.get();
        Thread.yield();
        Object o2 = delegate2.get();
        Assert.assertEquals("Unsafe Call to aggregator.get()", o1, o2);
        return o1;
      }

      @Override
      public float getFloat()
      {
        float o1 = delegate1.getFloat();
        Thread.yield();
        float o2 = delegate2.getFloat();
        Assert.assertTrue("Unsafe Call to aggregator.get()", o1 == o2);
        return o1;
      }

      @Override
      public void close()
      {
        delegate1.close();
        delegate2.close();
      }

      @Override
      public long getLong()
      {
        long o1 = delegate1.getLong();
        Thread.yield();
        long o2 = delegate2.getLong();
        Assert.assertEquals("Unsafe Call to aggregator.get()", o1, o2);
        return o1;
      }
    };
  }

  @Override
  public BufferAggregator factorizeBuffered(ColumnSelectorFactory metricFactory)
  {
    final BufferAggregator delegate1 = delegate.factorizeBuffered(metricFactory);
    final BufferAggregator delegate2 = delegate.factorizeBuffered(metricFactory);
    final int intermediateSize = delegate.getMaxIntermediateSize();
    return new BufferAggregator()
    {
      @Override
      public void init(ByteBuffer buf, int position)
      {
        delegate1.init(buf, position);
        delegate2.init(buf, position + intermediateSize);
      }

      @Override
      public void aggregate(ByteBuffer buf, int position)
      {
        delegate1.aggregate(buf, position);
        Thread.yield();
        delegate2.aggregate(buf, position + intermediateSize);
      }

      @Override
      public Object get(ByteBuffer buf, int position)
      {
        Object o1 = delegate1.get(buf, position);
        Thread.yield();
        Object o2 = delegate2.get(buf, position + intermediateSize);
        Assert.assertEquals("Unsafe Call to aggregator.get()", o1, o2);
        return o1;
      }

      @Override
      public float getFloat(ByteBuffer buf, int position)
      {
        float o1 = delegate1.getFloat(buf, position);
        Thread.yield();
        float o2 = delegate2.getFloat(buf, position + intermediateSize);
        Assert.assertTrue("Unsafe Call to aggregator.get()", o1 == o2);
        return o1;
      }

      @Override
      public void close()
      {
        delegate1.close();
        delegate2.close();
      }

      @Override
      public long getLong(ByteBuffer buf, int position)
      {
        long o1 = delegate1.getLong(buf, position);
        Thread.yield();
        long o2 = delegate2.getLong(buf, position + intermediateSize);
        Assert.assertEquals("Unsafe Call to aggregator.get()", o1, o2);
        return o1;
      }
    };
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
  public AggregatorFactory getCombiningFactory()
  {
    return delegate.getCombiningFactory();
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

  @Override
  public Object finalizeComputation(Object object)
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
    return 2 * delegate.getMaxIntermediateSize();
  }

  @Override
  public byte[] getCacheKey()
  {
    return delegate.getCacheKey();
  }
}
