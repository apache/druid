package org.apache.druid.segment.column;


import com.google.common.base.Supplier;

public interface ColumnPartSupplier<T> extends Supplier<T>
{
  default ColumnPartSize getColumnPartSize()
  {
    return ColumnPartSize.NO_DATA;
  }
}
