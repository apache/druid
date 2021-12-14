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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;

import java.util.Set;

/**
 * If an SQL query can be validated by {@link DruidPlanner}, the resulting artifact is the set of {@link Resource}
 * corresponding to the datasources and views which an authenticated request must be authorized for to process the
 * query.
 */
public class ValidationResult
{
  private final Set<ResourceAction> resourceActions;

  public ValidationResult(
      final Set<ResourceAction> resourceActions
  )
  {
    this.resourceActions = ImmutableSet.copyOf(resourceActions);
  }

  public Set<ResourceAction> getResourceActions()
  {
    return resourceActions;
  }
}
