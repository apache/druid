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

package com.metamx.druid.guice;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.metamx.druid.http.QueryServlet;
import com.metamx.druid.http.log.EmittingRequestLoggerProvider;
import com.metamx.druid.http.log.FileRequestLoggerProvider;
import com.metamx.druid.http.log.RequestLogger;
import com.metamx.druid.http.log.RequestLoggerProvider;
import com.metamx.druid.query.segment.QuerySegmentWalker;
import io.druid.initialization.DruidModule;

import java.util.Arrays;
import java.util.List;

/**
 */
public class QueryableModule implements DruidModule
{
  private final Class<? extends QuerySegmentWalker> walkerClass;

  public QueryableModule(
      Class<? extends QuerySegmentWalker> walkerClass
  )
  {
    this.walkerClass = walkerClass;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.bind(QueryServlet.class).in(LazySingleton.class);
    binder.bind(QuerySegmentWalker.class).to(walkerClass).in(LazySingleton.class);
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
