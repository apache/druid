package io.druid.query.aggregation;

public class Aggregators
{

  public static Aggregator synchronizedAggregator(Aggregator aggregator){
     return new SynchronizedAggregator(aggregator);
  }

  private static class SynchronizedAggregator implements Aggregator
  {

    private final Aggregator delegate;

    SynchronizedAggregator(Aggregator delegate)
    {
      this.delegate = delegate;
    }

    @Override
    public synchronized void aggregate()
    {
      delegate.aggregate();
    }

    @Override
    public synchronized void reset()
    {
      delegate.reset();
    }

    @Override
    public synchronized Object get()
    {
      return delegate.get();
    }

    @Override
    public synchronized float getFloat()
    {
      return delegate.getFloat();
    }

    @Override
    public synchronized String getName()
    {
      return delegate.getName();
    }

    @Override
    public synchronized void close()
    {
      delegate.close();
    }
  }
}
