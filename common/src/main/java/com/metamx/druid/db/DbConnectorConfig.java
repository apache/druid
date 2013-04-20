/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.db;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.skife.config.Config;

/**
 */
public abstract class DbConnectorConfig
{
  @JsonProperty("connectURI")
  @Config("druid.database.connectURI")
  public abstract String getDatabaseConnectURI();

  @JsonProperty("user")
  @Config("druid.database.user")
  public abstract String getDatabaseUser();

  @JsonProperty("password")
  @Config("druid.database.password")
  public abstract String getDatabasePassword();

  @JsonProperty("segmentTable")
  @Config("druid.database.segmentTable")
  public abstract String getSegmentTable();

  @JsonProperty("validationQuery")
  @Config("druid.database.validation")
  public boolean isValidationQuery() {
    return false;
  }

  @JsonProperty("validationQuery")
  @Config("druid.database.validationQuery")
  public String getValidationQuery() {
    return "SELECT 1";
  }
}
