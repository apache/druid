package org.apache.druid.query.operator;

import com.google.common.collect.Iterables;
import org.apache.druid.query.QueryProcessingPool;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.segment.Segment;

public class WindowOperatorQueryQueryRunnerFactory implements QueryRunnerFactory<RowsAndColumns, WindowOperatorQuery>
{
  public static final WindowOperatorQueryQueryToolChest TOOLCHEST = new WindowOperatorQueryQueryToolChest();

  @Override
  public QueryRunner<RowsAndColumns> createRunner(Segment segment)
  {
    return (queryPlus, responseContext) ->
        new OperatorSequence(() -> new SegmentToRowsAndColumnsOperator(segment));
  }

  @Override
  public QueryRunner<RowsAndColumns> mergeRunners(
      QueryProcessingPool queryProcessingPool, Iterable<QueryRunner<RowsAndColumns>> queryRunners
  )
  {
    return Iterables.getOnlyElement(queryRunners);
  }

  @Override
  public QueryToolChest<RowsAndColumns, WindowOperatorQuery> getToolchest()
  {
    return TOOLCHEST;
  }
}
