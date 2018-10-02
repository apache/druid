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

package org.apache.druid.server.emitter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.name.Named;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.java.util.common.lifecycle.Lifecycle;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.core.Emitter;
import org.apache.druid.java.util.emitter.core.ParametrizedUriEmitter;
import org.apache.druid.java.util.emitter.core.ParametrizedUriEmitterConfig;

import javax.annotation.Nullable;
import javax.net.ssl.SSLContext;

public class ParametrizedUriEmitterModule implements Module
{
  private static final Logger log = new Logger(ParametrizedUriEmitterModule.class);

  @Override
  public void configure(Binder binder)
  {
    JsonConfigProvider.bind(binder, "druid.emitter.parametrized", ParametrizedUriEmitterConfig.class);
    JsonConfigProvider.bind(binder, "druid.emitter.parametrized.httpEmitting", ParametrizedUriEmitterSSLClientConfig.class);
  }

  @Provides
  @ManageLifecycle
  @Named("parametrized")
  public Emitter getEmitter(
      Supplier<ParametrizedUriEmitterConfig> config,
      Supplier<ParametrizedUriEmitterSSLClientConfig> parametrizedSSLClientConfig,
      @Nullable SSLContext sslContext,
      Lifecycle lifecycle,
      ObjectMapper jsonMapper
  )
  {
    HttpEmitterSSLClientConfig sslConfig = parametrizedSSLClientConfig.get().getHttpEmittingSSLClientConfig();
    return new ParametrizedUriEmitter(
        config.get(),
        lifecycle.addCloseableInstance(
            HttpEmitterModule.createAsyncHttpClient(
                "ParmetrizedUriEmitter-AsyncHttpClient-%d",
                "ParmetrizedUriEmitter-AsyncHttpClient-Timer-%d",
                HttpEmitterModule.getEffectiveSSLContext(sslConfig, sslContext)
            )
        ),
        jsonMapper
    );
  }
}
