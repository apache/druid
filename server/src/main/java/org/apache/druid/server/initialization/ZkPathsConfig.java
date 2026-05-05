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

package org.apache.druid.server.initialization;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.curator.utils.ZKPaths;

public class ZkPathsConfig
{
  @JsonProperty
  private String base = "druid";
  @JsonProperty
  private String coordinatorPath;

  public String getBase()
  {
    return base;
  }

  public String getCoordinatorPath()
  {
    return (null == coordinatorPath) ? defaultPath("coordinator") : coordinatorPath;
  }

  public String getOverlordPath()
  {
    return defaultPath("overlord");
  }

  public String getInternalDiscoveryPath()
  {
    return defaultPath("internal-discovery");
  }

  public String defaultPath(final String subPath)
  {
    return ZKPaths.makePath(getBase(), subPath);
  }

  @Override
  public boolean equals(Object other)
  {
    if (null == other) {
      return false;
    }
    if (this == other) {
      return true;
    }
    if (!(other instanceof ZkPathsConfig)) {
      return false;
    }
    ZkPathsConfig otherConfig = (ZkPathsConfig) other;
    return this.getBase().equals(otherConfig.getBase()) &&
        this.getCoordinatorPath().equals(otherConfig.getCoordinatorPath());
  }

  @Override
  public int hashCode()
  {
    int result = base != null ? base.hashCode() : 0;
    result = 31 * result + (coordinatorPath != null ? coordinatorPath.hashCode() : 0);
    return result;
  }
}
