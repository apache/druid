/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.druid.java.util.common.StringUtils;

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

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + getConnectURI() + '\'' +
           ", user='" + user + '\'' +
           ", passwordProvider=" + passwordProvider +
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
    return result;
  }
}
