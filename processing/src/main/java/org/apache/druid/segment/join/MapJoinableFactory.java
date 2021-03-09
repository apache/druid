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
import org.apache.druid.com.google.common.collect.HashMultimap;
import org.apache.druid.com.google.common.collect.SetMultimap;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.DataSource;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

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
    return getSingleResult(dataSource, factory -> factory.build(dataSource, condition));
  }

  @Override
  public Optional<byte[]> computeJoinCacheKey(DataSource dataSource, JoinConditionAnalysis condition)
  {
    return getSingleResult(dataSource, factory -> factory.computeJoinCacheKey(dataSource, condition));
  }

  /**
   * Computes the given function assuming that only one joinable factory will return a non-empty result. If we get
   * results from two {@link JoinableFactory}, then throw an exception.
   *
   */
  private <T> Optional<T> getSingleResult(DataSource dataSource, Function<JoinableFactory, Optional<T>> function)
  {
    Set<JoinableFactory> factories = joinableFactories.get(dataSource.getClass());
    Optional<T> mayBeFinalResult = Optional.empty();
    for (JoinableFactory joinableFactory : factories) {
      Optional<T> candidate = function.apply(joinableFactory);
      if (candidate.isPresent() && mayBeFinalResult.isPresent()) {
        throw new ISE("Multiple joinable factories are valid for table[%s]", dataSource);
      }
      if (candidate.isPresent()) {
        mayBeFinalResult = candidate;
      }
    }
    return mayBeFinalResult;
  }
}
