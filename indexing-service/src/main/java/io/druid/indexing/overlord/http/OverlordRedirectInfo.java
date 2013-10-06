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

package io.druid.indexing.overlord.http;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.server.http.RedirectInfo;

import java.net.URL;

/**
*/
public class OverlordRedirectInfo implements RedirectInfo
{
  private final TaskMaster taskMaster;

  @Inject
  public OverlordRedirectInfo(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public boolean doLocal()
  {
    return taskMaster.isLeading();
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    try {
      return new URL(String.format("http://%s%s", taskMaster.getLeader(), requestURI));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }
}
