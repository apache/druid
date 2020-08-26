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
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * A {@link JoinableFactory} that delegates to the appropriate factory based on the datasource.
 *
 * Any number of {@link JoinableFactory} may be associated to the same class of {@link DataSource}, but for a specific
 * datasource only a single {@link JoinableFactory} should be able to create a {@link Joinable} in the {@link #build}
 * method.
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
    Optional<Joinable> maybeJoinable = Optional.empty();
    for (JoinableFactory factory : factories) {
      Optional<Joinable> candidate = factory.build(dataSource, condition);
      if (candidate.isPresent()) {
        if (maybeJoinable.isPresent()) {
          throw new ISE("Multiple joinable factories are valid for table[%s]", dataSource);
        }
        maybeJoinable = candidate;
      }
    }
    return maybeJoinable;
  }
}
