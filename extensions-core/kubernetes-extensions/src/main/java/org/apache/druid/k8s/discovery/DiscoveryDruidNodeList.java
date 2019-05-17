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

package org.apache.druid.k8s.discovery;

import com.google.common.base.Preconditions;
import org.apache.druid.discovery.DiscoveryDruidNode;

import java.util.Collections;
import java.util.Map;

public class DiscoveryDruidNodeList
{
  private final String resourceVersion;
  private final Map<String, DiscoveryDruidNode> druidNodes;

  public DiscoveryDruidNodeList(
      String resourceVersion,
      Map<String, DiscoveryDruidNode> druidNodes
  )
  {
    this.resourceVersion = Preconditions.checkNotNull(resourceVersion, "NULL resource version!");
    this.druidNodes = druidNodes == null ? Collections.EMPTY_MAP : druidNodes;
  }

  public String getResourceVersion()
  {
    return resourceVersion;
  }

  public Map<String, DiscoveryDruidNode> getDruidNodes()
  {
    return druidNodes;
  }
}
