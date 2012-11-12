package com.metamx.druid;

import com.metamx.druid.index.v1.serde.Registererer;

/**
 */
public interface RegisteringNode
{
  public void registerHandlers(Registererer... registererers);
}
