package com.metamx.druid.merger.common.index;

import java.util.Collection;
import java.util.Map;

public interface EventReceiver
{
  public void addAll(Collection<Map<String, Object>> events);
}
