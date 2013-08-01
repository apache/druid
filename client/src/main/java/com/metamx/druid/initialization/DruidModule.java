package com.metamx.druid.initialization;

import java.util.List;

/**
 */
public interface DruidModule extends com.google.inject.Module
{
  public List<com.fasterxml.jackson.databind.Module> getJacksonModules();
}
