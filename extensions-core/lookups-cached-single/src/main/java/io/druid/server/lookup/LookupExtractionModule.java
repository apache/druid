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

package io.druid.server.lookup;

import com.fasterxml.jackson.databind.Module;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;

import io.druid.initialization.DruidModule;
import io.druid.java.util.common.StringUtils;

import java.util.List;
import java.util.UUID;

public class LookupExtractionModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return ImmutableList.<Module>of(
        new SimpleModule("SingleCached-LoadingOrPolling-Lookup-Module")
        {
          @Override
          public void setupModule(SetupContext context)
          {
            context.registerSubtypes(LoadingLookupFactory.class);
            context.registerSubtypes(PollingLookupFactory.class);
          }
        }
    );
  }

  @Override
  public void configure(Binder binder)
  {
  }

  public static byte[] getRandomCacheKey()
  {
   return StringUtils.toUtf8(UUID.randomUUID().toString());
  }
}
