package org.apache.druid.query.operator.window;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.query.rowsandcols.AppendableRowsAndColumns;
import org.apache.druid.query.rowsandcols.DefaultOnHeapAggregatable;
import org.apache.druid.query.rowsandcols.OnHeapAggregatable;
import org.apache.druid.query.rowsandcols.OnHeapCumulativeAggregatable;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.ConstantObjectColumn;
import org.apache.druid.query.rowsandcols.column.ObjectArrayColumn;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

public class WindowAggregateProcessor
{
  @Nullable
  private static <T> List<T> emptyToNull(List<T> list) {
    if (list == null || list.isEmpty()) {
      return null;
    } else {
      return list;
    }
  }

  private final List<AggregatorFactory> aggregations;
  private final List<AggregatorFactory> cumulativeAggregations;

  public WindowAggregateProcessor(
      List<AggregatorFactory> aggregations,
      List<AggregatorFactory> cumulativeAggregations
  ) {
    this.aggregations = emptyToNull(aggregations);
    this.cumulativeAggregations = emptyToNull(cumulativeAggregations);
  }

  public RowsAndColumns process(RowsAndColumns inputPartition) {
    AppendableRowsAndColumns retVal = RowsAndColumns.expectAppendable(inputPartition);

    if (aggregations != null) {
      OnHeapAggregatable aggregatable = inputPartition.as(OnHeapAggregatable.class);
      if (aggregatable == null) {
        aggregatable = new DefaultOnHeapAggregatable(inputPartition);
      }
      final ArrayList<Object> aggregatedVals = aggregatable.aggregateAll(aggregations);

      for (int i = 0; i < aggregations.size(); ++i) {
        final AggregatorFactory agg = aggregations.get(i);
        retVal.addColumn(
            agg.getName(),
            new ConstantObjectColumn(aggregatedVals.get(i), inputPartition.numRows(), agg.getResultType())
        );
      }
    }

    if (cumulativeAggregations != null) {
      OnHeapCumulativeAggregatable cummulativeAgg = inputPartition.as(OnHeapCumulativeAggregatable.class);
      if (cummulativeAgg == null) {
        cummulativeAgg = new DefaultOnHeapAggregatable(inputPartition);
      }
      final ArrayList<Object[]> cumulativeVals = cummulativeAgg.aggregateCumulative(cumulativeAggregations);

      for (int i = 0; i < cumulativeAggregations.size(); ++i) {
        final AggregatorFactory agg = cumulativeAggregations.get(i);
        retVal.addColumn(agg.getName(), new ObjectArrayColumn(cumulativeVals.get(i), agg.getResultType()));
      }
    }

    return retVal;
  }
}
