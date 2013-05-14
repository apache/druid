package com.metamx.druid.query.group.orderby;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.metamx.common.guava.Sequence;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.aggregation.post.PostAggregator;
import com.metamx.druid.input.Row;
import com.metamx.druid.query.dimension.DimensionSpec;

import java.util.List;

/**
 */
public class NoopLimitSpec implements LimitSpec
{
  @Override
  public Function<Sequence<Row>, Sequence<Row>> build(
      List<DimensionSpec> dimensions, List<AggregatorFactory> aggs, List<PostAggregator> postAggs
  )
  {
    return Functions.identity();
  }
}
