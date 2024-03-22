package org.apache.druid.segment.column;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

public interface ColumnSupplier<TColumn extends BaseColumn> extends Supplier<TColumn>
{
  default Map<String, ColumnPartSize> getComponents()
  {
    return Collections.emptyMap();
  }

  class LegacyColumnSupplier<T extends BaseColumn> implements ColumnSupplier<T>
  {
    private final Supplier<T> supplier;

    public LegacyColumnSupplier(Supplier<T> supplier)
    {
      this.supplier = supplier;
    }

    @Override
    public T get()
    {
      return supplier.get();
    }
  }
}
