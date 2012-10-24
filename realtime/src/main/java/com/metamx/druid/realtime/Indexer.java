package com.metamx.druid.realtime;

import com.metamx.druid.input.InputRow;

/**
 */
public interface Indexer
{
  public int add(InputRow row);
}
