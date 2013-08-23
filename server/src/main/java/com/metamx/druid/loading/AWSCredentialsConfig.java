package com.metamx.druid.loading;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class AWSCredentialsConfig
{
  @JsonProperty
  private String accessKey = "";

  @JsonProperty
  private String secretKey = "";

  public String getAccessKey()
  {
    return accessKey;
  }

  public String getSecretKey()
  {
    return secretKey;
  }
}
