/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
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
