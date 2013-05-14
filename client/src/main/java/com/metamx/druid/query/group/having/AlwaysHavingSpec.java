package com.metamx.druid.query.group.having;

import com.metamx.druid.input.Row;

/**
 */
public class AlwaysHavingSpec implements HavingSpec
{
  @Override
  public boolean eval(Row row)
  {
    return true;
  }
}
