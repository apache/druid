package io.druid.query;

public interface ParallelQueryRunner<T> extends QueryRunner<T>
{

  /**
   * accumulator passed should be thread safe
   */
  <OutType> OutType runAndAccumulate(
      Query<T> query,
      OutType outType,
      com.metamx.common.guava.Accumulator<OutType, T> outTypeTAccumulator
  );
}
