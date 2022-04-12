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

package org.apache.druid.query.movingaverage;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.name.Named;
import org.apache.druid.discovery.NodeRole;
import org.apache.druid.guice.DruidBinders;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.Query;
import org.apache.druid.query.QuerySegmentWalker;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.server.NoopQuerySegmentWalker;
import org.apache.druid.server.log.NoopRequestLogger;
import org.apache.druid.server.log.RequestLogger;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class MovingAverageQueryModule implements DruidModule
{

  @Override
  public void configure(Binder binder)
  {
    MapBinder<Class<? extends Query>, QueryToolChest> toolChests = DruidBinders.queryToolChestBinder(binder);

    //Bind the query toolchest to the query class and add the binding to toolchest
    toolChests.addBinding(MovingAverageQuery.class).to(MovingAverageQueryToolChest.class);

    //Bind the query toolchest to binder
    binder.bind(MovingAverageQueryToolChest.class).in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.<Module>singletonList(new SimpleModule("MovingAverageQueryModule")
                                                 .registerSubtypes(new NamedType(
                                                     MovingAverageQuery.class,
                                                     "movingAverage"
                                                 )));
  }

  @Provides
  @LazySingleton
  @Named("movingAverage")
  public QuerySegmentWalker createQueryWalker(@Self Set<NodeRole> nodeRoles, Injector injector)
  {
    if (!nodeRoles.contains(NodeRole.BROKER)) {
      return new NoopQuerySegmentWalker();
    }
    return injector.getInstance(QuerySegmentWalker.class);
  }

  @Provides
  @LazySingleton
  @Named("movingAverage")
  public RequestLogger createRequestLogger(@Self Set<NodeRole> nodeRoles, Injector injector)
  {
    if (!nodeRoles.contains(NodeRole.BROKER)) {
      return new NoopRequestLogger();
    }
    return injector.getInstance(RequestLogger.class);
  }
}
