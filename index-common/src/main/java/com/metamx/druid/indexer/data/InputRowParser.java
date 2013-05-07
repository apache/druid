package com.metamx.druid.indexer.data;

import com.metamx.druid.input.InputRow;

public interface InputRowParser<T>
{
  public InputRow parse(T input);
  public void addDimensionExclusion(String dimension);
}
