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

package org.apache.druid.guice.http;

import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Global;
import org.apache.druid.guice.annotations.Self;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.server.DruidNode;
import org.apache.druid.server.metrics.HttpClientPoolRegistry;
import org.apache.druid.server.security.Escalator;
import org.apache.druid.server.security.NoopEscalator;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

/**
 * Covers the wiring that lets {@code HttpClientPoolMonitor} find the connection pools: every client of the process
 * hands its pool to the registry, under a name that is its own.
 */
public class HttpClientPoolRegistrationTest
{
  @Test
  public void testEachClientRegistersItsPoolUnderItsOwnName()
  {
    final Module testBindings = binder -> {
      binder.bind(Escalator.class).toInstance(NoopEscalator.getInstance());
      binder.bind(DruidNode.class)
            .annotatedWith(Self.class)
            .toInstance(new DruidNode("test", "localhost", false, 8080, null, true, false));
    };
    final Injector injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        List.of(testBindings, HttpClientModule.global(), HttpClientModule.escalatedGlobal())
    );

    injector.getInstance(Key.get(HttpClient.class, Global.class));
    injector.getInstance(Key.get(HttpClient.class, EscalatedGlobal.class));

    Assertions.assertEquals(
        Set.of("global", "escalatedGlobal"),
        injector.getInstance(HttpClientPoolRegistry.class).getPools().keySet()
    );
  }
}
