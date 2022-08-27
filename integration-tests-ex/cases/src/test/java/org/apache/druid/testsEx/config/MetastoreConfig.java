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

package org.apache.druid.testsEx.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;

import java.util.List;
import java.util.Map;

public class MetastoreConfig extends ServiceConfig
{
  /**
   * Driver. Defaults to the MySQL Driver.
   * @see {@link org.apache.druid.metadata.storage.mysql.MySQLConnectorDriverConfig}
   */
  private final String driver;

  /**
   * JDBC connection URI. Required.
   */
  private final String connectURI;

  /**
   * User for the metastore DB.
   */
  private final String user;

  /**
   * Password for the metastore DB.
   */
  private final String password;

  /**
   * Optional connection properties.
   */
  private final Map<String, Object> properties;

  @JsonCreator
  public MetastoreConfig(
      @JsonProperty("service") String service,
      @JsonProperty("driver") String driver,
      @JsonProperty("connectURI") String connectURI,
      @JsonProperty("user") String user,
      @JsonProperty("password") String password,
      @JsonProperty("properties") Map<String, Object> properties,
      @JsonProperty("instances") List<ServiceInstance> instances
  )
  {
    super(service, instances);
    this.driver = driver;
    this.connectURI = connectURI;
    this.user = user;
    this.password = password;
    this.properties = properties;
  }

  @JsonProperty("driver")
  @JsonInclude(Include.NON_NULL)
  public String driver()
  {
    return driver;
  }

  @JsonProperty("connectURI")
  @JsonInclude(Include.NON_NULL)
  public String connectURI()
  {
    return connectURI;
  }

  @JsonProperty("user")
  @JsonInclude(Include.NON_NULL)
  public String user()
  {
    return user;
  }

  @JsonProperty("password")
  @JsonInclude(Include.NON_NULL)
  public String password()
  {
    return password;
  }

  @JsonProperty("properties")
  @JsonInclude(Include.NON_NULL)
  public Map<String, Object> properties()
  {
    return properties;
  }

  public boolean validate(List<String> errs)
  {
    if (Strings.isNullOrEmpty(connectURI)) {
      errs.add("Metastore connect URI is required");
      return false;
    }
    return true;
  }
}
