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

package com.metamx.druid.indexer.updater;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 */
public class ZkUpdaterJobSpec implements UpdaterJobSpec
{
  @JsonProperty("zkHosts")
  public String zkQuorum;

  @JsonProperty("zkBasePath")
  private String zkBasePath;

  public ZkUpdaterJobSpec() {}

  public String getZkQuorum()
  {
    return zkQuorum;
  }

  public String getZkBasePath()
  {
    return zkBasePath;
  }

  public boolean postToZk()
  {
    return !(zkQuorum == null || zkBasePath == null);
  }
}
