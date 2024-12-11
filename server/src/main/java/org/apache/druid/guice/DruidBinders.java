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

public class DruidBinders
{
  public static MapBinder<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactoryBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends Query>>() {},
        TypeLiteral.get(QueryRunnerFactory.class)
    );
  }

  public static MapBinderHelper<Class<? extends Query>, QueryRunnerFactory> queryRFBinder(Binder binder)
  {
    return new MapBinderHelper<>(binder, queryRunnerFactoryBinder(binder));
  }

  public static MapBinder<Class<? extends Query>, QueryToolChest> queryToolChestBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends Query>>() {},
        new TypeLiteral<QueryToolChest>() {}
    );
  }

  public static MapBinderHelper<Class<? extends Query>, QueryToolChest> queryTCBinder(Binder binder)
  {
    return new MapBinderHelper<>(binder, queryToolChestBinder(binder));
  }

  public static MapBinder<Class<? extends Query>, QueryLogic> queryLogicBinderType(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends Query>>() {},
        new TypeLiteral<QueryLogic>() {}
    );
  }

  public static QueryLogicBinder queryLogicBinder(Binder binder)
  {
    return new QueryLogicBinder(binder);
  }

  public static class QueryLogicBinder extends MapBinderHelper<Class<? extends Query>, QueryLogic>
  {
    public QueryLogicBinder(Binder binder)
    {
      super(binder, DruidBinders.queryLogicBinderType(binder));
    }

    QueryLogicBinder bindQueryLogic(
        Class<? extends Query> queryTypeClazz,
        Class<? extends QueryLogic> queryLogicClazz)
    {
      naiveBinding(queryTypeClazz, queryLogicClazz);
      return this;
    }
  }


  public static Multibinder<KeyHolder<DruidNode>> discoveryAnnouncementBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<KeyHolder<DruidNode>>() {});
  }

  public static Multibinder<Class<? extends Monitor>> metricMonitorBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<Class<? extends Monitor>>() {});
  }

  public static MapBinder<Class<? extends DataSource>, SegmentWrangler> segmentWranglerBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends DataSource>>() {},
        new TypeLiteral<SegmentWrangler>() {}
    );
  }

  public static Multibinder<JoinableFactory> joinableFactoryMultiBinder(Binder binder)
  {
    return Multibinder.newSetBinder(
        binder,
        new TypeLiteral<JoinableFactory>() {}
    );
  }

  public static MapBinder<Class<? extends JoinableFactory>, Class<? extends DataSource>> joinableMappingBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder,
        new TypeLiteral<Class<? extends JoinableFactory>>() {},
        new TypeLiteral<Class<? extends DataSource>>() {}
    );
  }

  public static class MapBinderHelper<KeyClass, ValueClass>
  {
    private final Binder binder;
    private final MapBinder<KeyClass, ValueClass> mapBinder;

    private MapBinderHelper(
        Binder binder,
        MapBinder<KeyClass, ValueClass> mapBinder
    )
    {
      this.binder = binder;
      this.mapBinder = mapBinder;
    }

    public Binder getBinder()
    {
      return binder;
    }

    public MapBinder<KeyClass, ValueClass> getMapBinder()
    {
      return mapBinder;
    }

    /**
     * Just binds with the MapBinder, so that modules can control how the actual object is injected
     *
     * @param key
     * @param value
     * @return
     */
    public MapBinderHelper<KeyClass, ValueClass> mapOnlyBind(KeyClass key, Class<? extends ValueClass> value)
    {
      mapBinder.addBinding(key).to(value);
      return this;
    }

    /**
     * Bundles the map binding and the binding of the object.  The actual object will be bound directly in
     * LazySingleton scope.  It is important to realize that this binding will be used instead of, e.g. provider
     * methods on the module.  So if you want to control how the object is instantiated beyond just using an
     * @Inject constructor, use mapOnlyBind instead.
     *
     * @param key
     * @param value
     * @return
     */
    public MapBinderHelper<KeyClass, ValueClass> naiveBinding(KeyClass key, Class<? extends ValueClass> value)
    {
      mapOnlyBind(key, value);
      binder.bind(value).in(LazySingleton.class);
      return this;
    }
  }
}
