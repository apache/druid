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

package org.apache.druid.metadata;

import org.apache.druid.audit.AuditInfo;
import org.apache.druid.server.coordinator.rules.RetentionRulesSnapshot;
import org.apache.druid.server.coordinator.rules.Rule;

import java.util.List;

/**
 */
public interface MetadataRuleManager
{
  void start();

  void stop();

  void poll();

  /**
   * Current snapshot of the rules of all datasources.
   * <p>
   * Using a single snapshot while performing an operation (such as a Coordinator duty run) allows the steps within the
   * operation to remain consistent with each other, even if the rules are updated concurrently.
   */
  RetentionRulesSnapshot getRulesSnapshot();

  boolean overrideRule(String dataSource, List<Rule> rulesConfig, AuditInfo auditInfo);

  /**
   * Remove rules for non-existence datasource (datasource with no segment) created older than the given timestamp.
   *
   * @param timestamp timestamp in milliseconds
   * @return number of rules removed
   */
  int removeRulesForEmptyDatasourcesOlderThan(long timestamp);
}
