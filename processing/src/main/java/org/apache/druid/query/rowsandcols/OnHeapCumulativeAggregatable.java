package org.apache.druid.query.rowsandcols;

import org.apache.druid.query.aggregation.AggregatorFactory;

import java.util.ArrayList;
import java.util.List;

public interface OnHeapCumulativeAggregatable
{
  ArrayList<Object[]> aggregateCumulative(List<AggregatorFactory> aggFactories);
}
