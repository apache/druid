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

package org.apache.druid.indexing.overlord.http;

import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import org.apache.commons.io.FilenameUtils;
import org.apache.druid.indexing.overlord.TaskMaster;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.http.RedirectInfo;

import java.net.URL;
import java.util.Iterator;
import java.util.Set;

/**
 */
public class OverlordRedirectInfo implements RedirectInfo
{
  private static final Set<String> LOCAL_PATHS = ImmutableSet.of(
      "/druid/indexer/v1/leader",
      "/druid/indexer/v1/isLeader"
  );

  private static final Set<String> LOCAL_GET_PATHS = ImmutableSet.of(
      "/druid/indexer/v1/task/*/log",
      "/druid/indexer/v1/task/*/reports",
      "/druid/indexer/v1/task/*/segments",
      "/druid/indexer/v1/worker/history"
  );

  private final TaskMaster taskMaster;

  @Inject
  public OverlordRedirectInfo(TaskMaster taskMaster)
  {
    this.taskMaster = taskMaster;
  }

  @Override
  public boolean doLocal(String requestURI)
  {
    return (requestURI != null && LOCAL_PATHS.contains(requestURI)) || taskMaster.isLeader();
  }

  @Override
  public boolean doLocalGet(String requestURI)
  {
    if (taskMaster.isLeader()) {
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
      final String leader = taskMaster.getCurrentLeader();
      if (leader == null || leader.isEmpty()) {
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
