/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.server.http;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.coordinator.DruidCoordinator;

import java.net.URL;
import java.util.Iterator;
import java.util.Set;

/**
 */
public class CoordinatorRedirectInfo implements RedirectInfo
{
  private static final Set<String> LOCAL_PATHS = ImmutableSet.of(
      "/druid/coordinator/v1/leader",
      "/druid/coordinator/v1/isLeader"
  );

  private static final Set<String> LOCAL_GET_PATHS = ImmutableSet.of(
    "/druid-ext/basic-security/authentication/db",
    "/druid-ext/basic-security/authorization/db"
  );

  private final DruidCoordinator coordinator;

  @Inject
  public CoordinatorRedirectInfo(DruidCoordinator coordinator)
  {
    this.coordinator = coordinator;
  }

  @Override
  public boolean doLocal(String requestURI)
  {
    return (requestURI != null && LOCAL_PATHS.contains(requestURI)) || coordinator.isLeader();
  }

  @Override
  public boolean doLocalGet(String requestURI)
  {
    if (coordinator.isLeader()) {
      return true;
    }

    if (requestURI != null) {
      // If the pattern matched exactly, return true
      if (LOCAL_GET_PATHS.contains(requestURI)) {
        return true;
      }

      // If the glob pattern matched, return true
      Iterator<String> it = LOCAL_GET_PATHS.iterator();

      while (it.hasNext()) {
        if (FilenameUtils.wildcardMatch(requestURI, it.next())) {
          return true;
        }
      }
    }
    return false;
  }

  @Override
  public URL getRedirectURL(String queryString, String requestURI)
  {
    try {
      final String leader = coordinator.getCurrentLeader();
      if (leader == null) {
        return null;
      }

      String location = StringUtils.format("%s%s", leader, requestURI);

      if (queryString != null) {
        location = StringUtils.format("%s?%s", location, queryString);
      }

      return new URL(location);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
