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

package org.apache.druid.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.druid.java.util.common.StringUtils;
import java.util.Properties;

/**
 */
public class MetadataStorageConnectorConfig
{
  @JsonProperty
  private boolean createTables = true;

  @JsonProperty
  private String host = "localhost";

  @JsonProperty
  private int port = 1527;

  @JsonProperty
  private String connectURI;

  @JsonProperty
  private String user = null;

  @JsonProperty("password")
  private PasswordProvider passwordProvider;

  @JsonProperty("dbcp")
  private Properties dbcpProperties;

  public boolean isCreateTables()
  {
    return createTables;
  }

  public String getHost()
  {
    return host;
  }

  public int getPort()
  {
    return port;
  }

  @JsonProperty
  public String getConnectURI()
  {
    if (connectURI == null) {
      return StringUtils.format("jdbc:derby://%s:%s/druid;create=true", host, port);
    }
    return connectURI;
  }

  public String getUser()
  {
    return user;
  }

  public String getPassword()
  {
    return passwordProvider == null ? null : passwordProvider.getPassword();
  }

  public Properties getDbcpProperties()
  {
    return dbcpProperties;
  }

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + getConnectURI() + '\'' +
           ", user='" + user + '\'' +
           ", passwordProvider=" + passwordProvider +
           ", dbcpProperties=" + dbcpProperties +
           '}';
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (!(o instanceof MetadataStorageConnectorConfig)) {
      return false;
    }

    MetadataStorageConnectorConfig that = (MetadataStorageConnectorConfig) o;

    if (isCreateTables() != that.isCreateTables()) {
      return false;
    }
    if (getPort() != that.getPort()) {
      return false;
    }
    if (getHost() != null ? !getHost().equals(that.getHost()) : that.getHost() != null) {
      return false;
    }
    if (getConnectURI() != null ? !getConnectURI().equals(that.getConnectURI()) : that.getConnectURI() != null) {
      return false;
    }
    if (getUser() != null ? !getUser().equals(that.getUser()) : that.getUser() != null) {
      return false;
    }
    if (getDbcpProperties() == null
        ? that.getDbcpProperties() != null
        : !getDbcpProperties().equals(that.getDbcpProperties())) {
      return false;
    }
    return passwordProvider != null ? passwordProvider.equals(that.passwordProvider) : that.passwordProvider == null;

  }

  @Override
  public int hashCode()
  {
    int result = (isCreateTables() ? 1 : 0);
    result = 31 * result + (getHost() != null ? getHost().hashCode() : 0);
    result = 31 * result + getPort();
    result = 31 * result + (getConnectURI() != null ? getConnectURI().hashCode() : 0);
    result = 31 * result + (getUser() != null ? getUser().hashCode() : 0);
    result = 31 * result + (passwordProvider != null ? passwordProvider.hashCode() : 0);
    result = 31 * result + (dbcpProperties != null ? dbcpProperties.hashCode() : 0);
    return result;
  }
}
