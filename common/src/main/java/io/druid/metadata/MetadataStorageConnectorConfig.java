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

package io.druid.metadata;

import com.fasterxml.jackson.annotation.JsonProperty;

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

  @JsonProperty
  private String password = null;

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
      return String.format("jdbc:derby://%s:%s/druid;create=true", host, port);
    }
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

  @Override
  public String toString()
  {
    return "DbConnectorConfig{" +
           "createTables=" + createTables +
           ", connectURI='" + getConnectURI() + '\'' +
           ", user='" + user + '\'' +
           ", password=****" +
           '}';
  }
}
