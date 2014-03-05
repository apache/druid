package io.druid.query.aggregation.hyperloglog;

import io.druid.query.aggregation.Aggregator;
import io.druid.segment.ObjectColumnSelector;

/**
 */
public class HyperUniquesAggregator implements Aggregator
{
  private final String name;
  private final ObjectColumnSelector selector;

  private HyperLogLogCollector collector;

  public HyperUniquesAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public void aggregate()
  {
    collector.fold((HyperLogLogCollector) selector.get());
  }

  @Override
  public void reset()
  {
    collector = HyperLogLogCollector.makeLatestCollector();
  }

  @Override
  public Object get()
  {
    return collector;
  }

  @Override
  public float getFloat()
  {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public Aggregator clone()
  {
    return new HyperUniquesAggregator(name, selector);
  }

  @Override
  public void close()
  {
    // no resources to cleanup
  }
}
