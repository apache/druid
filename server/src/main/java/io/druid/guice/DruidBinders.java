/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.guice;

import com.google.inject.Binder;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import com.google.inject.multibindings.Multibinder;
import com.metamx.metrics.Monitor;
import io.druid.query.Query;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryToolChest;
import io.druid.server.DruidNode;

/**
 */
public class DruidBinders
{
  public static MapBinder<Class<? extends Query>, QueryRunnerFactory> queryRunnerFactoryBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder, new TypeLiteral<Class<? extends Query>>(){}, TypeLiteral.get(QueryRunnerFactory.class)
    );
  }

  public static MapBinder<Class<? extends Query>, QueryToolChest> queryToolChestBinder(Binder binder)
  {
    return MapBinder.newMapBinder(
        binder, new TypeLiteral<Class<? extends Query>>(){}, new TypeLiteral<QueryToolChest>(){}
    );
  }

  public static Multibinder<KeyHolder<DruidNode>> discoveryAnnouncementBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<KeyHolder<DruidNode>>(){});
  }

  public static Multibinder<Class<? extends Monitor>> metricMonitorBinder(Binder binder)
  {
    return Multibinder.newSetBinder(binder, new TypeLiteral<Class<? extends Monitor>>(){});
  }
}
