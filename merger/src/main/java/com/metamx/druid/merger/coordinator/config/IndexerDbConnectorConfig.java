package com.metamx.druid.merger.coordinator.config;

import com.metamx.druid.db.DbConnectorConfig;
import org.codehaus.jackson.annotate.JsonProperty;
import org.skife.config.Config;

public abstract class IndexerDbConnectorConfig extends DbConnectorConfig
{
  @JsonProperty("taskTable")
  @Config("druid.database.taskTable")
  public abstract String getTaskTable();
}
