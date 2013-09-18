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

package io.druid.indexer.updater;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Supplier;
import io.druid.db.DbConnectorConfig;

/**
 */
public class DbUpdaterJobSpec implements Supplier<DbConnectorConfig>
{
  @JsonProperty("connectURI")
  public String connectURI;

  @JsonProperty("user")
  public String user;

  @JsonProperty("password")
  public String password;

  @JsonProperty("segmentTable")
  public String segmentTable;

  public String getSegmentTable()
  {
    return segmentTable;
  }

  @Override
  public DbConnectorConfig get()
  {
    return new DbConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return connectURI;
      }

      @Override
      public String getUser()
      {
        return user;
      }

      @Override
      public String getPassword()
      {
        return password;
      }
    };
  }
}
