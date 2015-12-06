/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
