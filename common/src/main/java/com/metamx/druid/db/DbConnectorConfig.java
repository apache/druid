package com.metamx.druid.db;

import org.codehaus.jackson.annotate.JsonProperty;
import org.skife.config.Config;

/**
 */
public abstract class DbConnectorConfig
{
  @JsonProperty("connectURI")
  @Config("druid.database.connectURI")
  public abstract String getDatabaseConnectURI();

  @JsonProperty("user")
  @Config("druid.database.user")
  public abstract String getDatabaseUser();

  @JsonProperty("password")
  @Config("druid.database.password")
  public abstract String getDatabasePassword();

  @JsonProperty("segmentTable")
  @Config("druid.database.segmentTable")
  public abstract String getSegmentTable();
}
