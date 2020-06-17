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

import com.google.inject.Inject;
import org.apache.druid.query.DataSource;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * A {@link JoinableFactory} that delegates to the appropriate factory based on the type of the datasource.
 *
 * Datasources can register a factory via a DruidBinder
 */
public class MapJoinableFactory implements JoinableFactory
{
  private final Map<Class<? extends DataSource>, JoinableFactory> joinableFactories;

  @Inject
  public MapJoinableFactory(Map<Class<? extends DataSource>, JoinableFactory> joinableFactories)
  {
    // Accesses to IdentityHashMap should be faster than to HashMap or ImmutableMap.
    // Class doesn't override Object.equals().
    this.joinableFactories = new IdentityHashMap<>(joinableFactories);
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    JoinableFactory factory = joinableFactories.get(dataSource.getClass());
    if (factory == null) {
      return false;
    } else {
      return factory.isDirectlyJoinable(dataSource);
    }
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    JoinableFactory factory = joinableFactories.get(dataSource.getClass());
    if (factory == null) {
      return Optional.empty();
    } else {
      return factory.build(dataSource, condition);
    }
  }
}
