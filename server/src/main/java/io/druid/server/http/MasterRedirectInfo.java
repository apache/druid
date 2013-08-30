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

package io.druid.server.http;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.druid.master.DruidMaster;

import java.net.URL;

/**
*/
public class MasterRedirectInfo implements RedirectInfo
{
  private final DruidMaster master;

  @Inject
  public MasterRedirectInfo(DruidMaster master) {
    this.master = master;
  }

  @Override
  public boolean doLocal()
  {
    return master.isClusterMaster();
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    try {
      final String currentMaster = master.getCurrentMaster();
      if (currentMaster == null) {
        return null;
      }

      String location = String.format("http://%s%s", currentMaster, requestURI);

      if (queryString != null) {
        location = String.format("%s?%s", location, queryString);
      }

      return new URL(location);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
