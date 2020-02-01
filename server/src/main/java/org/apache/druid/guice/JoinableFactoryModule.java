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

package org.apache.druid.guice;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Scopes;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.InlineDataSource;
import org.apache.druid.query.LookupDataSource;
import org.apache.druid.segment.join.InlineJoinableFactory;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.segment.join.LookupJoinableFactory;
import org.apache.druid.segment.join.MapJoinableFactory;

import java.util.Map;

/**
 * Module that installs {@link JoinableFactory} for the appropriate DataSource.
 */
public class JoinableFactoryModule implements Module
{
  /**
   * Default mappings of datasources to factories.
   */
  @VisibleForTesting
  static final Map<Class<? extends DataSource>, Class<? extends JoinableFactory>> FACTORY_MAPPINGS =
      ImmutableMap.of(
          InlineDataSource.class, InlineJoinableFactory.class,
          LookupDataSource.class, LookupJoinableFactory.class
      );

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends DataSource>, JoinableFactory> joinableFactories =
        DruidBinders.joinableFactoryBinder(binder);

    FACTORY_MAPPINGS.forEach((ds, factory) -> {
      joinableFactories.addBinding(ds).to(factory);
      binder.bind(factory).in(LazySingleton.class);
    });

    binder.bind(JoinableFactory.class).to(MapJoinableFactory.class)
          .in(Scopes.SINGLETON);
  }
}
