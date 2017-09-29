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

package io.druid.server.initialization;

import com.fasterxml.jackson.databind.Module;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.druid.guice.LazySingleton;
import io.druid.initialization.DruidModule;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.security.AuthenticatorHttpClientWrapper;
import io.druid.server.security.AuthenticatorMapper;

import java.util.Collections;
import java.util.List;

public class AuthenticatorHttpClientWrapperModule implements DruidModule
{
  private static Logger log = new Logger(AuthenticatorHttpClientWrapperModule.class);

  @Override
  public void configure(Binder binder)
  {
    binder.bind(AuthenticatorHttpClientWrapper.class)
          .toProvider(new AuthenticatorHttpClientWrapperProvider())
          .in(LazySingleton.class);
  }

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return Collections.EMPTY_LIST;
  }

  private static class AuthenticatorHttpClientWrapperProvider implements Provider<AuthenticatorHttpClientWrapper>
  {
    private AuthenticatorHttpClientWrapper wrapper;

    @Inject
    public void inject(AuthenticatorMapper authenticatorMapper)
    {
      this.wrapper = new AuthenticatorHttpClientWrapper(authenticatorMapper.getEscalatingAuthenticator());
    }

    @Override
    public AuthenticatorHttpClientWrapper get()
    {
      return wrapper;
    }
  }
}
