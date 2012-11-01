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

package com.metamx.druid.coordination;

import java.util.Arrays;
import java.util.Map;

import com.metamx.druid.master.DruidMaster;
import com.metamx.phonebook.PhoneBook;

/**
 */
public class DruidClusterInfo
{
  private final DruidClusterInfoConfig config;
  private final PhoneBook yp;

  public DruidClusterInfo(
      DruidClusterInfoConfig config,
      PhoneBook zkPhoneBook
  )
  {
    this.config = config;
    this.yp = zkPhoneBook;
  }

  public Map<String, String> lookupCurrentLeader()
  {
    return (Map<String, String>) yp.lookup(
        yp.combineParts(Arrays.asList(config.getMasterPath(), DruidMaster.MASTER_OWNER_NODE)), Map.class
    );
  }

  public String getMasterHost()
  {
    return lookupCurrentLeader().get("host");
  }
}
