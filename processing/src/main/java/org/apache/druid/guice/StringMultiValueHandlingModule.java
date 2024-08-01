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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DefaultColumnFormatConfig;

import javax.annotation.Nonnull;

/**
 * Module to determine the default mode of string multi value handling.
 */
public class StringMultiValueHandlingModule implements DruidModule
{
  @Override
  public void configure(Binder binder)
  {
    // binding our side effect class to the lifecycle causes setStringMultiValueHandlingMode to be called on service
    // start, allowing use of the config to get the system default multi value handling mode.
    LifecycleModule.register(binder, SideEffectHandlerRegisterer.class);
  }

  /**
   * The system property along with the default is managed in {@link DefaultColumnFormatConfig} itself.
   * So the value is guaranteed to be non-null after lifecycle service start.
   */
  @Nonnull
  private static DimensionSchema.MultiValueHandling STRING_MV_MODE;

  /**
   * @return the configured string multi value handling mode from the system config if set; otherwise, returns
   * the default.
   */
  public static DimensionSchema.MultiValueHandling getConfiguredOrDefaultStringMvMode()
  {
    return STRING_MV_MODE;
  }

  @Provides
  @LazySingleton
  public static SideEffectHandlerRegisterer setStringMvMode(DefaultColumnFormatConfig formatsConfig)
  {
    STRING_MV_MODE = formatsConfig.getStringMultiValueHandlingMode();
    return new SideEffectHandlerRegisterer();
  }

  /**
   * Helper for wiring stuff up for tests that don't use guice injection.
   */
  @VisibleForTesting
  public static void initializeStringMvForTests()
  {
    STRING_MV_MODE = DimensionSchema.MultiValueHandling.SORTED_ARRAY;
  }

  /**
   * This is used as a vehicle to register the correct version of the system default string mvd mode by side
   * effect with the help of binding to {@link org.apache.druid.java.util.common.lifecycle.Lifecycle} so that
   * {@link #setStringMvMode(DefaultColumnFormatConfig)} can be called with the injected
   * {@link DefaultColumnFormatConfig}.
   */
  public static class SideEffectHandlerRegisterer
  {
    // nothing to see here
  }
}
