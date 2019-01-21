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

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.query.DefaultQueryRunnerFactoryConglomerate;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.server.log.ComposingRequestLoggerProvider;
import org.apache.druid.server.log.EmittingRequestLoggerProvider;
import org.apache.druid.server.log.FileRequestLoggerProvider;
import org.apache.druid.server.log.FilteredRequestLoggerProvider;
import org.apache.druid.server.log.LoggingRequestLoggerProvider;
import org.apache.druid.server.log.NoopRequestLoggerProvider;
import org.apache.druid.server.log.RequestLogger;
import org.apache.druid.server.log.RequestLoggerProvider;
import org.apache.druid.server.log.SwitchingRequestLoggerProvider;

import java.util.Collections;
import java.util.List;

/**
 */
public class QueryableModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    binder.bind(RequestLogger.class).toProvider(RequestLoggerProvider.class).in(ManageLifecycle.class);
    JsonConfigProvider.bindWithDefault(
        binder,
        "druid.request.logging",
        RequestLoggerProvider.class,
        NoopRequestLoggerProvider.class
    );

    binder.bind(QueryRunnerFactoryConglomerate.class)
          .to(DefaultQueryRunnerFactoryConglomerate.class)
          .in(LazySingleton.class);
  }

  @Override
  public List<Module> getJacksonModules()
  {
    return Collections.singletonList(
        new SimpleModule("QueryableModule")
            .registerSubtypes(
                EmittingRequestLoggerProvider.class,
                FileRequestLoggerProvider.class,
                LoggingRequestLoggerProvider.class,
                ComposingRequestLoggerProvider.class,
                FilteredRequestLoggerProvider.class,
                SwitchingRequestLoggerProvider.class
            )
    );
  }
}
