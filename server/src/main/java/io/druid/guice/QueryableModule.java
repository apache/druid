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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.util.Providers;
import io.druid.initialization.DruidModule;
import io.druid.query.QuerySegmentWalker;
import io.druid.server.log.EmittingRequestLoggerProvider;
import io.druid.server.log.FileRequestLoggerProvider;
import io.druid.server.log.RequestLogger;
import io.druid.server.log.RequestLoggerProvider;

import java.util.Arrays;
import java.util.List;

/**
 */
public class QueryableModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(QuerySegmentWalker.class).toProvider(Providers.<QuerySegmentWalker>of(null));
    binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bind(binder, "druid.request.logging", RequestLoggerProvider.class);
  }

  @Override
  public List<Module> getJacksonModules()
  {
    return Arrays.<Module>asList(
        new SimpleModule("QueryableModule")
            .registerSubtypes(EmittingRequestLoggerProvider.class, FileRequestLoggerProvider.class)
    );
  }
}
