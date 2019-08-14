package org.apache.druid.segment.selector.settable;

import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.BaseFloatColumnValueSelector;
import org.apache.druid.segment.ColumnValueSelector;

/**
 * A BaseFloatColumnValueSelector impl to return settable float value on calls to
 * {@link ColumnValueSelector#getFloat()}
 */
public class SettableValueFloatColumnValueSelector implements BaseFloatColumnValueSelector
{
  private float value;

  @Override
  public float getFloat()
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

  public void setValue(float value)
  {
    this.value = value;
  }
}
