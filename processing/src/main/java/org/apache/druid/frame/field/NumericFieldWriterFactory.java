package org.apache.druid.frame.field;

import org.apache.druid.segment.ColumnValueSelector;

public interface NumericFieldWriterFactory
{
  NumericFieldWriter get(ColumnValueSelector<Number> selector);
}
