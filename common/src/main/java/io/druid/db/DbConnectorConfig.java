/*
 * Druid - a distributed column store.
 * Copyright 2012 - 2015 Metamarkets Group Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.druid.db;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.validation.constraints.NotNull;

/**
 */
public class DbConnectorConfig
{
  @JsonProperty
  private boolean createTables = true;

  @JsonProperty
  @NotNull
  private String connectURI = null;

  @JsonProperty
  @NotNull
  private String user = null;

  @JsonProperty
  @NotNull
  private String password = null;

  @JsonProperty
  private boolean useValidationQuery = false;

  @JsonProperty
  private String validationQuery = "SELECT 1";

  public boolean isCreateTables()
  {
    return createTables;
  }

  public String getConnectURI()
  {
    return connectURI;
  }

  public String getUser()
  {
    return user;
  }

  public String getPassword()
  {
    return password;
  }

  public boolean isUseValidationQuery()
  {
    return useValidationQuery;
  }

  public String getValidationQuery() {
    return validationQuery;
  }

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + connectURI + '\'' +
           ", user='" + user + '\'' +
           ", password=****" +
           ", useValidationQuery=" + useValidationQuery +
           ", validationQuery='" + validationQuery + '\'' +
           '}';
  }
}
