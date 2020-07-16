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

package org.apache.druid.segment.join;

import org.apache.druid.query.DataSource;

import java.util.Optional;

/**
 * Utility for creating {@link Joinable} objects.
 *
 * @see org.apache.druid.guice.DruidBinders#joinableFactoryBinder to register factories
 */
public interface JoinableFactory
{
  /**
   * Returns true if a {@link Joinable} **may** be created for a given {@link DataSource}, but is not a guarantee that
   * {@link #build} will return a non-empty result. Successfully building a {@link Joinable} might require specific
   * criteria of the {@link JoinConditionAnalysis}.
   */
  boolean isDirectlyJoinable(DataSource dataSource);

  /**
   * Create a Joinable object. This may be an expensive operation involving loading data, creating a hash table, etc.
   *
   * @param dataSource the datasource to join on
   * @param condition  the condition to join on
   *
   * @return a Joinable if this datasource + condition combo is joinable; empty if not
   */
  Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition);
}
