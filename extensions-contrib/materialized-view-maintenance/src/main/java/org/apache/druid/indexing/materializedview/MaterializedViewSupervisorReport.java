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

package org.apache.druid.indexing.materializedview;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Sets;
import org.apache.druid.indexing.overlord.supervisor.SupervisorReport;
import org.apache.druid.indexing.overlord.supervisor.SupervisorStateManager;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.util.List;
import java.util.Set;

public class MaterializedViewSupervisorReport extends SupervisorReport
{
  public MaterializedViewSupervisorReport(
      String dataSource,
      DateTime generationTime,
      boolean suspended,
      String baseDataSource,
      Set<String> dimensions,
      Set<String> metrics,
      List<Interval> missTimeline,
      boolean healthy,
      SupervisorStateManager.State state,
      List<SupervisorStateManager.ExceptionEvent> recentErrors
  )
  {
    super(
        dataSource,
        generationTime,
        ImmutableMap.builder()
                    .put("dataSource", dataSource)
                    .put("baseDataSource", baseDataSource)
                    .put("suspended", suspended)
                    .put("dimensions", dimensions)
                    .put("metrics", metrics)
                    .put("missTimeline", Sets.newHashSet(missTimeline))
                    .put("healthy", healthy)
                    .put("state", state)
                    .put("recentErrors", recentErrors)
                    .build()
    );
  }
}
