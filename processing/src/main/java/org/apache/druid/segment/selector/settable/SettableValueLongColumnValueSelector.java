package org.apache.druid.segment.selector.settable;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseLongColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * A BaseLongColumnValueSelector impl to return settable long value on calls to
 * {@link ColumnValueSelector#getLong()}
 */
public class SettableValueLongColumnValueSelector implements BaseLongColumnValueSelector
{
  private long value;

  @Override
  public long getLong()
  {
    return value;
  }

  @Override
  public void inspectRuntimeShape(RuntimeShapeInspector inspector)
  {

  }

  @Override
  public boolean isNull()
  {
    return false;
  }

  public void setValue(long value)
  {
    this.value = value;
  }
}

