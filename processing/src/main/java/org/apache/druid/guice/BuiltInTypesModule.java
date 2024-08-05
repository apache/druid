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
import com.fasterxml.jackson.databind.jsontype.NamedType;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Binder;
import com.google.inject.Provides;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.DimensionHandler;
import org.apache.druid.segment.DimensionHandlerProvider;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.NestedCommonFormatColumnHandler;
import org.apache.druid.segment.NestedDataColumnHandlerV4;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataJsonSerializer;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class BuiltInTypesModule implements DruidModule
{
  /**
   * Initialized with a default value so tests can just get it via {@link #getStringMultiValueHandlingMode} without any
   * explicit initialization. In production, this default may be overridden if a value is configured via
   * {@link #initDimensionHandlerAndMvHandlingMode(DefaultColumnFormatConfig)}.
   */
  private static DimensionSchema.MultiValueHandling STRING_MV_MODE = DimensionSchema.MultiValueHandling.SORTED_ARRAY;

  @Override
  public List<? extends Module> getJacksonModules()
  {
    return getJacksonModulesList();
  }

  @Override
  public void configure(Binder binder)
  {
    registerSerde();
    // binding our side effect classes to the lifecycle causes the initDimensionHandlerAndMvHandlingMode to be
    // called on service start, allowing use of the config to get the system default format version and string multi
    // value handling mode.
    LifecycleModule.register(binder, SideEffectRegisterer.class);
  }

  @Provides
  @LazySingleton
  public SideEffectRegisterer initDimensionHandlerAndMvHandlingMode(DefaultColumnFormatConfig formatsConfig)
  {
    if (formatsConfig.getNestedColumnFormatVersion() != null && formatsConfig.getNestedColumnFormatVersion() == 4) {
      DimensionHandlerUtils.registerDimensionHandlerProvider(
          NestedDataComplexTypeSerde.TYPE_NAME,
          new NestedColumnV4HandlerProvider()
      );
    } else {
      DimensionHandlerUtils.registerDimensionHandlerProvider(
          NestedDataComplexTypeSerde.TYPE_NAME,
          new NestedCommonFormatHandlerProvider()
      );
    }

    setStringMultiValueHandlingModeIfConfigured(formatsConfig.getStringMultiValueHandlingMode());
    return new SideEffectRegisterer();
  }

  private static void setStringMultiValueHandlingModeIfConfigured(@Nullable String stringMultiValueHandlingMode)
  {
    if (stringMultiValueHandlingMode != null) {
      STRING_MV_MODE = DimensionSchema.MultiValueHandling.fromString(stringMultiValueHandlingMode);
    }
  }

  /**
   * @return the configured string multi value handling mode from the system config if set; otherwise, returns
   * the default.
   */
  public static DimensionSchema.MultiValueHandling getStringMultiValueHandlingMode()
  {
    return STRING_MV_MODE;
  }

  public static List<SimpleModule> getJacksonModulesList()
  {
    return Collections.singletonList(
        new SimpleModule("NestedDataModule")
            .registerSubtypes(new NamedType(NestedFieldVirtualColumn.class, "nested-field"))
            .addSerializer(StructuredData.class, new StructuredDataJsonSerializer())
    );
  }

  /**
   * Helper for wiring stuff up for tests
   */
  @VisibleForTesting
  public static void registerHandlersAndSerde()
  {
    registerSerde();
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        NestedDataComplexTypeSerde.TYPE_NAME,
        new NestedCommonFormatHandlerProvider()
    );
  }

  private static void registerSerde()
  {
    if (ComplexMetrics.getSerdeForType(NestedDataComplexTypeSerde.TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);
    }
  }

  public static class NestedCommonFormatHandlerProvider
      implements DimensionHandlerProvider<StructuredData, StructuredData, StructuredData>
  {

    @Override
    public DimensionHandler<StructuredData, StructuredData, StructuredData> get(String dimensionName)
    {
      return new NestedCommonFormatColumnHandler(dimensionName, null);
    }
  }

  public static class NestedColumnV4HandlerProvider
      implements DimensionHandlerProvider<StructuredData, StructuredData, StructuredData>
  {

    @Override
    public DimensionHandler<StructuredData, StructuredData, StructuredData> get(String dimensionName)
    {
      return new NestedDataColumnHandlerV4(dimensionName);
    }
  }

  /**
   * this is used as a vehicle to register the correct version of the system default nested column handler and multi
   * value handling mode by side effect with the help of binding to
   * {@link org.apache.druid.java.util.common.lifecycle.Lifecycle} so that
   * {@link #initDimensionHandlerAndMvHandlingMode(DefaultColumnFormatConfig)} can be called with the injected
   * {@link DefaultColumnFormatConfig}.
   */
  public static class SideEffectRegisterer
  {
    // nothing to see here
  }
}
