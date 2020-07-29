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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import com.google.inject.Inject;
import org.apache.druid.query.DataSource;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link JoinableFactory} that delegates to the appropriate factory based on the type of the datasource.
 *
 * Datasources can register a factory via a DruidBinder. Any number of factories can be bound to a datasource, the
 * 'first' that matches will be returned to the caller, or none if no matches.
 */
public class MapJoinableFactory implements JoinableFactory
{
  private final SetMultimap<Class<? extends DataSource>, JoinableFactory> joinableFactories;

  @Inject
  public MapJoinableFactory(
      Set<JoinableFactory> factories,
      Map<Class<? extends JoinableFactory>, Class<? extends DataSource>> factoryToDataSource
  )
  {
    this.joinableFactories = HashMultimap.create();
    factories.forEach(joinableFactory -> {
      joinableFactories.put(factoryToDataSource.get(joinableFactory.getClass()), joinableFactory);
    });
  }

  @Override
  public boolean isDirectlyJoinable(DataSource dataSource)
  {
    Set<JoinableFactory> factories = joinableFactories.get(dataSource.getClass());
    for (JoinableFactory factory : factories) {
      if (factory.isDirectlyJoinable(dataSource)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Optional<Joinable> build(DataSource dataSource, JoinConditionAnalysis condition)
  {
    Set<JoinableFactory> factories = joinableFactories.get(dataSource.getClass());
    for (JoinableFactory factory : factories) {
      Optional<Joinable> maybeJoinable = factory.build(dataSource, condition);
      if (maybeJoinable.isPresent()) {
        return maybeJoinable;
      }
    }
    return Optional.empty();
  }
}
