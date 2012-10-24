package com.metamx.druid.coordination.legacy;

import java.util.Map;

/**
 */
public interface SizeLookup
{
  public Long lookupSize(Map<String, Object> descriptor);
}
