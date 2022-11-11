package org.apache.druid.query.operator.window;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
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

public class WindowAggregateProcessor implements Processor
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

  @JsonCreator
  public WindowAggregateProcessor(
      @JsonProperty("aggregations") List<AggregatorFactory> aggregations,
      @JsonProperty("cumulativeAggregations") List<AggregatorFactory> cumulativeAggregations
  ) {
    this.aggregations = emptyToNull(aggregations);
    this.cumulativeAggregations = emptyToNull(cumulativeAggregations);
  }

  @JsonProperty("aggregations")
  public List<AggregatorFactory> getAggregations()
  {
    return aggregations;
  }

  @JsonProperty("cumulativeAggregations")
  public List<AggregatorFactory> getCumulativeAggregations()
  {
    return cumulativeAggregations;
  }

  @Override
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
