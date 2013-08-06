package com.metamx.druid.client.cache;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

public class MemcachedCacheConfig
{
  @JsonProperty
  private int expiration = 2592000; // What is this number?

  @JsonProperty
  private int timeout = 500;

  @JsonProperty
  @NotNull
  private String hosts;

  @JsonProperty
  private int maxObjectSize = 50 * 1024 * 1024;

  @JsonProperty
  private String memcachedPrefix = "druid";

  public int getExpiration()
  {
    return expiration;
  }

  public int getTimeout()
  {
    return timeout;
  }

  public String getHosts()
  {
    return hosts;
  }

  public int getMaxObjectSize()
  {
    return maxObjectSize;
  }

  public String getMemcachedPrefix()
  {
    return memcachedPrefix;
  }
}
