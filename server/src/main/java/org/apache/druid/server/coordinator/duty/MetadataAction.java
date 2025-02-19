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

package org.apache.druid.server.coordinator.duty;

import org.apache.druid.server.coordinator.rules.Rule;
import org.apache.druid.timeline.SegmentId;

import java.util.List;
import java.util.Set;

/**
 * Contains functional interfaces that are used by a {@link CoordinatorDuty} to
 * perform a single read or write operation on the metadata store.
 */
public final class MetadataAction
{
  @FunctionalInterface
  public interface DeleteSegments
  {
    int markSegmentsAsUnused(String datasource, Set<SegmentId> segmentIds);
  }

  @FunctionalInterface
  public interface GetDatasourceRules
  {
    List<Rule> getRulesWithDefault(String dataSource);
  }
}
