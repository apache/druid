package com.metamx.druid.indexer.updater;

import com.metamx.druid.db.DbConnectorConfig;
import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class DbUpdaterJobSpec extends DbConnectorConfig implements UpdaterJobSpec
{
  @JsonProperty("connectURI")
  public String connectURI;

  @JsonProperty("user")
  public String user;

  @JsonProperty("password")
  public String password;

  @JsonProperty("segmentTable")
  public String segmentTable;

  @Override
  public String getDatabaseConnectURI()
  {
    return connectURI;
  }

  @Override
  public String getDatabaseUser()
  {
    return user;
  }

  @Override
  public String getDatabasePassword()
  {
    return password;
  }

  @Override
  public String getSegmentTable()
  {
    return segmentTable;
  }
}
