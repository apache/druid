package org.apache.druid.query.aggregation;

public abstract class MaxIntermediateSizeAdjustStrategy
{

  /**
   * Specify the number of aggregates in a list that need to be sorted from small to large
   *
   * @return
   */
  public abstract int[] adjustWithRollupNum();

  /**
   * When the specified number of aggregate bars is reached, AppenatorImpl#bytesCurrentlyInMemory
   * The size to be appended: bytesCurrentlyInMemory + appendBytesOnRollupNum.
   *
   * @return
   */
  public abstract int[] appendBytesOnRollupNum();

  /**
   * GetMaxIntermediateSize + initAppendBytes: The number of bytes required to create an Agg object (usually negative)
   *
   * @return
   */
  public abstract int initAppendBytes();
}
