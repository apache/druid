package org.apache.druid.query.aggregation.bloom;

import org.apache.druid.math.expr.ExprEval;
import org.apache.druid.math.expr.ExpressionType;
import org.apache.druid.query.filter.BloomKFilter;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.column.TypeSignature;
import org.apache.druid.segment.column.ValueType;

import java.nio.ByteBuffer;

public class ByteBloomFilterAggregator extends BaseBloomFilterAggregator<BaseObjectColumnValueSelector<Object>>
{
  private final ExpressionType columnType;

  ByteBloomFilterAggregator(
      BaseObjectColumnValueSelector<Object> baseObjectColumnValueSelector,
      TypeSignature<ValueType> columnType,
      int maxNumEntries,
      boolean onHeap
  )
  {
    super(baseObjectColumnValueSelector, maxNumEntries, onHeap);
    this.columnType = ExpressionType.fromColumnTypeStrict(columnType);
  }

  @Override
  void bufferAdd(ByteBuffer buf)
  {
    final Object val = selector.getObject();
    if (val == null) {
      BloomKFilter.addBytes(buf, null, 0, 0);
    } else {
      BloomKFilter.addBytes(buf, ExprEval.toBytes(columnType, val));
    }
  }
}
