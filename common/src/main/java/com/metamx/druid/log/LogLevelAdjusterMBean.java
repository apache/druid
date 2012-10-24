package com.metamx.druid.log;

/**
 */
public interface LogLevelAdjusterMBean
{
  public String getLevel(String packageName);
  public void setLevel(String packageName, String level);
}
