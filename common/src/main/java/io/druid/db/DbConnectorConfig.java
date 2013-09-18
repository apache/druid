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
