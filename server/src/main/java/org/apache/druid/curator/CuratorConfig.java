/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.curator;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;

import javax.validation.constraints.Min;

public class CuratorConfig
{
  static final String HOST = "host";
  @JsonProperty(HOST)
  private String zkHosts = "localhost";

  @JsonProperty("sessionTimeoutMs")
  @Min(0)
  private int zkSessionTimeoutMs = 30_000;

  static final String CONNECTION_TIMEOUT_MS = "connectionTimeoutMs";
  @JsonProperty(CONNECTION_TIMEOUT_MS)
  @Min(0)
  private int zkConnectionTimeoutMs = 15_000;  // same as Curator default: https://git.io/fjhhr

  @JsonProperty("compress")
  private boolean enableCompression = true;

  @JsonProperty("acl")
  private boolean enableAcl = false;

  @JsonProperty("user")
  private String zkUser;

  @JsonProperty("pwd")
  private PasswordProvider zkPwd = new DefaultPasswordProvider("");

  @JsonProperty("authScheme")
  private String authScheme = "digest";

  public String getZkHosts()
  {
    return zkHosts;
  }

  public void setZkHosts(String zkHosts)
  {
    this.zkHosts = zkHosts;
  }

  public int getZkSessionTimeoutMs()
  {
    return zkSessionTimeoutMs;
  }

  public void setZkSessionTimeoutMs(Integer zkSessionTimeoutMs)
  {
    this.zkSessionTimeoutMs = zkSessionTimeoutMs;
  }

  public int getZkConnectionTimeoutMs()
  {
    return zkConnectionTimeoutMs;
  }

  public void setZkConnectionTimeoutMs(Integer zkConnectionTimeoutMs)
  {
    this.zkConnectionTimeoutMs = zkConnectionTimeoutMs;
  }

  public boolean getEnableCompression()
  {
    return enableCompression;
  }

  public void setEnableCompression(Boolean enableCompression)
  {
    Preconditions.checkNotNull(enableCompression, "enableCompression");
    this.enableCompression = enableCompression;
  }

  public boolean getEnableAcl()
  {
    return enableAcl;
  }

  public void setEnableAcl(Boolean enableAcl)
  {
    Preconditions.checkNotNull(enableAcl, "enableAcl");
    this.enableAcl = enableAcl;
  }

  public String getZkUser()
  {
    return zkUser;
  }

  public String getZkPwd()
  {
    return zkPwd.getPassword();
  }

  public String getAuthScheme()
  {
    return authScheme;
  }
}
