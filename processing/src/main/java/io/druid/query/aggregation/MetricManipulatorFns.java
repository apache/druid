package io.druid.query.aggregation;

/**
 */
public class MetricManipulatorFns
{
  public static MetricManipulationFn identity()
  {
    return new MetricManipulationFn()
    {
      @Override
      public Object manipulate(AggregatorFactory factory, Object object)
      {
        return object;
      }
    };
  }

  public static MetricManipulationFn finalizing()
  {
    return new MetricManipulationFn()
    {
      @Override
      public Object manipulate(AggregatorFactory factory, Object object)
      {
        return factory.finalizeComputation(object);
      }
    };
  }

  public static MetricManipulationFn deserializing()
  {
    return new MetricManipulationFn()
    {
      @Override
      public Object manipulate(AggregatorFactory factory, Object object)
      {
        return factory.deserialize(object);
      }
    };
  }
}
