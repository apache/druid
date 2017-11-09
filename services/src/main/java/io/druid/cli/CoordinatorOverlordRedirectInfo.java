/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.cli;

import com.google.inject.Inject;
import io.druid.indexing.overlord.TaskMaster;
import io.druid.indexing.overlord.http.OverlordRedirectInfo;
import io.druid.server.coordinator.DruidCoordinator;
import io.druid.server.http.CoordinatorRedirectInfo;
import io.druid.server.http.RedirectInfo;

import java.net.URL;

/**
 */
public class CoordinatorOverlordRedirectInfo implements RedirectInfo
{
  private final OverlordRedirectInfo overlordRedirectInfo;
  private final CoordinatorRedirectInfo coordinatorRedirectInfo;

  @Inject
  public CoordinatorOverlordRedirectInfo(TaskMaster taskMaster, DruidCoordinator druidCoordinator)
  {
    this.overlordRedirectInfo = new OverlordRedirectInfo(taskMaster);
    this.coordinatorRedirectInfo = new CoordinatorRedirectInfo(druidCoordinator);
  }

  @Override
  public boolean doLocal(String requestURI)
  {
    return isOverlordRequest(requestURI) ?
           overlordRedirectInfo.doLocal(requestURI) :
           coordinatorRedirectInfo.doLocal(requestURI);
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    return isOverlordRequest(requestURI) ?
           overlordRedirectInfo.getRedirectURL(queryString, requestURI) :
           coordinatorRedirectInfo.getRedirectURL(queryString, requestURI);
  }

  private boolean isOverlordRequest(String requestURI)
  {
    return requestURI.startsWith("/druid/indexer");
  }
}
