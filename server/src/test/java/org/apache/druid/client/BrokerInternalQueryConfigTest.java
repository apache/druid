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

package org.apache.druid.client;

import com.fasterxml.jackson.core.io.JsonEOFException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Provides;
import org.apache.druid.guice.ConfigModule;
import org.apache.druid.guice.DruidGuiceExtensions;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.segment.TestHelper;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class BrokerInternalQueryConfigTest
{
  private static final ObjectMapper MAPPER = TestHelper.makeJsonMapper();

  @Test
  public void testSerde() throws Exception
  {
    //defaults
    String json = "{}";

    BrokerInternalQueryConfig config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
    );

    Assert.assertEquals(ImmutableMap.of(), config.getContext());

    //non-defaults
    json = "{ \"context\": {\"priority\": 5}}";

    config = MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(json, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
    );

    Map<String, Object> expected = new HashMap<>();
    expected.put("priority", 5);
    Assert.assertEquals(expected, config.getContext());
  }

  /**
   * Malformatted configuration will trigger an exception and fail to startup the service
   *
   * @throws Exception
   */
  @Test(expected = JsonEOFException.class)
  public void testMalfomattedContext() throws Exception
  {
    String malformedJson = "{\"priority: 5}";
    MAPPER.readValue(
        MAPPER.writeValueAsString(
            MAPPER.readValue(malformedJson, BrokerInternalQueryConfig.class)
        ),
        BrokerInternalQueryConfig.class
    );
  }

  /**
   * Test the behavior if the operator does not specify anything for druid.broker.internal.query.config.context in runtime.properties
   */
  @Test
  public void testDefaultBehavior()
  {
    Injector injector = Guice.createInjector(
        new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            binder.install(new ConfigModule());
            binder.install(new DruidGuiceExtensions());
            JsonConfigProvider.bind(binder, "druid.broker.internal.query.config", BrokerInternalQueryConfig.class);
          }

          @Provides
          @LazySingleton
          public ObjectMapper jsonMapper()
          {
            return new DefaultObjectMapper();
          }
        }
    );
    BrokerInternalQueryConfig config = injector.getInstance(BrokerInternalQueryConfig.class);
    Assert.assertEquals(ImmutableMap.of(), config.getContext());
  }
}
