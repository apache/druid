package com.metamx.druid.indexer.data;

import com.metamx.common.exception.FormattedException;
import com.metamx.druid.input.InputRow;

public interface InputRowParser<T>
{
  public InputRow parse(T input) throws FormattedException;
  public void addDimensionExclusion(String dimension);
}
