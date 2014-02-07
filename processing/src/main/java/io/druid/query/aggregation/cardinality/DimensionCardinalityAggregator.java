package io.druid.query.aggregation.cardinality;

import com.google.common.base.Charsets;
import com.metamx.common.logger.Logger;
import io.druid.query.aggregation.Aggregator;
import io.druid.query.aggregation.cardinality.hll.HyperLogLogPlus;
import io.druid.segment.ObjectColumnSelector;

import java.util.List;

/**
 *
 */
public class DimensionCardinalityAggregator implements Aggregator
{
  private static final Logger log = new Logger(DimensionCardinalityAggregator.class);

  static final int MAX_SIZE_BYTES = 1376;

  static final HyperLogLogPlus makeHllPlus()
  {
    return new HyperLogLogPlus(11);
  }

  private final String name;
  private final ObjectColumnSelector selector;

  private volatile HyperLogLogPlus hllPlus = null;

  public DimensionCardinalityAggregator(
      String name,
      ObjectColumnSelector selector
  )
  {
    this.name = name;
    this.selector = selector;

    this.hllPlus = makeHllPlus();
  }

  @Override
  public void aggregate()
  {
    Object obj = selector.get();
    hllPlus.offer(obj);
  }

  @Override
  public void reset()
  {
    hllPlus = makeHllPlus();
  }

  @Override
  public Object get()
  {
    return hllPlus;
  }

  @Override
  public float getFloat()
  {
    return hllPlus.cardinality();
  }

  @Override
  public String getName()
  {
    return name;
  }

  @Override
  public void close()
  {

  }
}
