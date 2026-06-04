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

package org.apache.druid.server.coordinator.rules;

import org.apache.druid.server.coordinator.duty.RunRules;

/**
 * Result of a {@link Rule#run} invocation. Rules return {@link #OK} when there is nothing for the {@link RunRules}
 * duty to do beyond the actions the rule has already queued on the {@link SegmentActionHandler}. Rules that need
 * follow-up coordination across siblings in a shard group return a typed implementation (today only
 * {@link ShardGroupFollowup}); the {@link RunRules} duty dispatches on the concrete type and applies it at an
 * appropriate point during iteration.
 * <p>
 * To add a new kind of post-processing, implement this interface and add a dispatch branch in {@link RunRules}.
 */
public interface RuleRunResult
{
  RuleRunResult OK = new RuleRunResult()
  {
  };
}
