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

package org.apache.druid.sql.calcite.rule;

import org.apache.calcite.plan.RelOptRule;
import org.apache.druid.guice.annotations.UnstableApi;
import org.apache.druid.sql.calcite.planner.PlannerContext;

/**
 * This interface provides a way to supply custom calcite planning rules from extensions. All the custom rules are
 * collected and supplied to the planner which invokes {@link ExtensionCalciteRuleProvider#getRule(PlannerContext)}
 * for each of the rule provider per query.
 */
@UnstableApi
public interface ExtensionCalciteRuleProvider
{
  RelOptRule getRule(PlannerContext plannerContext);
}
