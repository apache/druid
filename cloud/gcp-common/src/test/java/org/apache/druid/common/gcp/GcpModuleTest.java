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

package org.apache.druid.common.gcp;

import com.google.api.client.googleapis.testing.auth.oauth2.MockGoogleCredential;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.testing.http.MockHttpTransport;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Scopes;
import com.google.inject.util.Modules;
import org.apache.druid.guice.LazySingleton;
import org.junit.Assert;
import org.junit.Test;

public class GcpModuleTest
{
  @Test
  public void testSimpleModuleLoads()
  {
    final Injector injector = Guice.createInjector(Modules.override(new GcpModule()).with(new GcpMockModule()
    {
      @Override
      public void configure(Binder binder)
      {
        binder.bindScope(LazySingleton.class, Scopes.SINGLETON);
      }
    }));
    Assert.assertTrue(injector.getInstance(HttpRequestInitializer.class) instanceof MockGoogleCredential);
    Assert.assertTrue(injector.getInstance(HttpTransport.class) instanceof MockHttpTransport);
  }
}
