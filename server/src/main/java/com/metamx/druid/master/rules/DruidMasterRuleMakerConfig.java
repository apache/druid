package com.metamx.druid.master.rules;

import org.skife.config.Config;

/**
 */
public abstract class DruidMasterRuleMakerConfig
{
  @Config("druid.database.ruleTable")
  public abstract String getRuleTable();

  @Config("druid.database.defaultDatasource")
  public abstract String getDefaultDatasource();
}
