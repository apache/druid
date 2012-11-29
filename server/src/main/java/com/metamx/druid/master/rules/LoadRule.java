package com.metamx.druid.master.rules;

/**
 */
public interface LoadRule extends Rule
{
  public abstract int getReplicationFactor();

  public abstract String getNodeType();
}
