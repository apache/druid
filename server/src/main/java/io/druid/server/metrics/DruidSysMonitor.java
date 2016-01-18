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

package io.druid.server.metrics;

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.metamx.metrics.SysMonitor;
import io.druid.segment.loading.SegmentLoaderConfig;
import io.druid.segment.loading.StorageLocationConfig;

import java.util.List;

/**
 */
public class DruidSysMonitor extends SysMonitor
{
  @Inject
  public DruidSysMonitor(
      SegmentLoaderConfig config
  )
  {
    final List<StorageLocationConfig> locs = config.getLocations();
    List<String> dirs = Lists.newArrayListWithExpectedSize(locs.size());
    for (StorageLocationConfig loc : locs) {
      dirs.add(loc.getPath().toString());
    }

    addDirectoriesToMonitor(dirs.toArray(new String[dirs.size()]));
  }
}
