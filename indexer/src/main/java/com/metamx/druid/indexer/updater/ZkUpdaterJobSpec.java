package com.metamx.druid.indexer.updater;

import org.codehaus.jackson.annotate.JsonProperty;

/**
 */
public class ZkUpdaterJobSpec implements UpdaterJobSpec
{
  @JsonProperty("zkHosts")
  public String zkQuorum;

  @JsonProperty("zkBasePath")
  private String zkBasePath;

  public ZkUpdaterJobSpec() {}

  public String getZkQuorum()
  {
    return zkQuorum;
  }

  public String getZkBasePath()
  {
    return zkBasePath;
  }

  public boolean postToZk()
  {
    return !(zkQuorum == null || zkBasePath == null);
  }
}
