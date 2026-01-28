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

import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import org.apache.druid.java.util.metrics.Monitor;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryLogic;
import org.apache.druid.query.QueryRunnerFactory;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.segment.SegmentWrangler;
import org.apache.druid.segment.join.JoinableFactory;
import org.apache.druid.server.DruidNode;

import java.lang.annotation.Annotation;
import java.util.Set;

public class DruidBinders
{
  public static MapBinder<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactoryBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        TypeLiteral.get(QueryRunnerFactory.class)
    );
  }

  public static MapBinder<Class<? extends Query>, QueryToolChest> queryToolChestBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  public static MapBinder<Class<? extends Query>, QueryLogic> queryLogicBinderType(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  public static QueryBinder queryBinder(Binder binder)
  {
    return new QueryBinder(binder);
  }

  public static class QueryBinder
  {
    MapBinderHelper<Class<? extends Query>, QueryLogic> queryLogicBinder;
    MapBinderHelper<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactoryBinder;
    MapBinderHelper<Class<? extends Query>, QueryToolChest> queryToolChestBinder;

    public QueryBinder(Binder binder)
    {
      queryLogicBinder = new MapBinderHelper<>(
          binder,
          queryLogicBinderType(binder),
          ImmutableSet.of(LazySingleton.class)
      );
      queryRunnerFactoryBinder = new MapBinderHelper<>(
          binder,
          queryRunnerFactoryBinder(binder),
          ImmutableSet.of(LazySingleton.class)
      );
      queryToolChestBinder = new MapBinderHelper<>(
          binder,
          queryToolChestBinder(binder),
          ImmutableSet.of(LazySingleton.class)
      );
    }

    public QueryBinder bindQueryLogic(
        Class<? extends Query> queryTypeClazz,
        Class<? extends QueryLogic> queryLogicClazz)
    {
      queryLogicBinder.bind(queryTypeClazz, queryLogicClazz);
      return this;
    }

    public QueryBinder bindQueryRunnerFactory(
        Class<? extends Query> queryTypeClazz,
        Class<? extends QueryRunnerFactory> queryRunnerFactory)
    {
      queryRunnerFactoryBinder.bind(queryTypeClazz, queryRunnerFactory);
      return this;
    }

    public QueryBinder naiveBinding2(Class<? extends Query> class1, Class<? extends QueryToolChest> class2)
    {
      return bindQueryToolChest(class1, class2);
    }

    public QueryBinder bindQueryToolChest(
        Class<? extends Query> queryTypeClazz,
        Class<? extends QueryToolChest> queryToolChest)
    {
      queryToolChestBinder.bind(queryTypeClazz, queryToolChest);
      return this;
    }

    public QueryBinder naiveBinding(
        Class<? extends Query> queryTypeClazz,
        Class<? extends QueryRunnerFactory> queryRunnerFactory)

    {
      return bindQueryRunnerFactory(queryTypeClazz, queryRunnerFactory);
    }
  }

  public static Multibinder<KeyHolder<DruidNode>> discoveryAnnouncementBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<>() {});
  }

  public static Multibinder<Class<? extends Monitor>> metricMonitorBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<>() {});
  }

  public static MapBinder<Class<? extends DataSource>, SegmentWrangler> segmentWranglerBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  public static Multibinder<JoinableFactory> joinableFactoryMultiBinder(Binder binder)
  {
    return Multibinder.newSetBinder(
        binder,
        new TypeLiteral<>() {}
    );
  }

  public static MapBinder<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableMappingBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<>() {},
        new TypeLiteral<>() {}
    );
  }

  public static class MapBinderHelper<KeyClass, ValueClass>
  {
    protected final Binder binder;
    protected final MapBinder<KeyClass, ValueClass> mapBinder;
    protected final Set<Class<? extends Annotation>> scopes;

    private MapBinderHelper(
        Binder binder,
        MapBinder<KeyClass, ValueClass> mapBinder,
        Set<Class<? extends Annotation>> scopes)
    {
      this.binder = binder;
      this.mapBinder = mapBinder;
      this.scopes = scopes;
    }

    protected Binder getBinder()
    {
      return binder;
    }

    protected MapBinder<KeyClass, ValueClass> getMapBinder()
    {
      return mapBinder;
    }

    public MapBinderHelper<KeyClass, ValueClass> bind(KeyClass key, Class<? extends ValueClass> value)
    {
      mapBinder.addBinding(key).to(value);
      for (Class<? extends Annotation> clazz : scopes) {
        binder.bind(value).in(clazz);
      }
      return this;
    }
  }
}
