package com.metamx.druid.input;

import java.util.List;

/**
 */
public interface InputRow extends Row
{
  public List<String> getDimensions();
}
