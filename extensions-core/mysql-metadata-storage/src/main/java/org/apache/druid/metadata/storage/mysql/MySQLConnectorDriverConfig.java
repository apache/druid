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

package org.apache.druid.metadata.storage.mysql;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public class MySQLConnectorDriverConfig
{
  public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
  public static final String MARIA_DB_DRIVER = "org.mariadb.jdbc.Driver";

  @JsonProperty
  private String driverClassName = MYSQL_DRIVER;

  @JsonProperty
  public String getDriverClassName()
  {
    return driverClassName;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MySQLConnectorDriverConfig that = (MySQLConnectorDriverConfig) o;
    return driverClassName.equals(that.driverClassName);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(driverClassName);
  }

  @Override
  public String toString()
  {
    return "MySQLConnectorDriverConfig{" +
           "driverClassName='" + driverClassName + '\'' +
           '}';
  }
}
