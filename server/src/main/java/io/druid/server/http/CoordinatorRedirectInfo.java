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

package io.druid.server.http;

import com.google.common.base.Throwables;
import com.google.inject.Inject;
import io.druid.server.coordinator.DruidCoordinator;

import java.net.URL;

/**
*/
public class CoordinatorRedirectInfo implements RedirectInfo
{
  private final DruidCoordinator coordinator;

  @Inject
  public CoordinatorRedirectInfo(DruidCoordinator coordinator) {
    this.coordinator = coordinator;
  }

  @Override
  public boolean doLocal()
  {
    return coordinator.isLeader();
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    try {
      final String leader = coordinator.getCurrentLeader();
      if (leader == null) {
        return null;
      }

      String location = String.format("http://%s%s", leader, requestURI);

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
