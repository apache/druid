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
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.NestedDataDimensionHandler;
import org.apache.druid.segment.NestedDataDimensionSchema;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.apache.druid.segment.nested.StructuredData;
import org.apache.druid.segment.nested.StructuredDataJsonSerializer;
import org.apache.druid.segment.serde.ComplexMetrics;
import org.apache.druid.segment.virtual.NestedFieldVirtualColumn;

import java.util.Collections;
import java.util.List;

public class NestedDataModule implements DruidModule
{
  @Override
  public List<? extends Module> getJacksonModules()
  {
    return getJacksonModulesList();
  }

  @Override
  public void configure(Binder binder)
  {
    registerHandlersAndSerde();
  }


  @VisibleForTesting
  public static void registerHandlersAndSerde()
  {
    if (ComplexMetrics.getSerdeForType(NestedDataComplexTypeSerde.TYPE_NAME) == null) {
      ComplexMetrics.registerSerde(NestedDataComplexTypeSerde.TYPE_NAME, NestedDataComplexTypeSerde.INSTANCE);

    }
    DimensionHandlerUtils.registerDimensionHandlerProvider(
        NestedDataComplexTypeSerde.TYPE_NAME,
        NestedDataDimensionHandler::new
    );
  }

  public static List<SimpleModule> getJacksonModulesList()
  {
    return Collections.singletonList(
        new SimpleModule("NestedDataModule")
            .registerSubtypes(
                new NamedType(NestedDataDimensionSchema.class, NestedDataComplexTypeSerde.TYPE_NAME),
                new NamedType(NestedFieldVirtualColumn.class, "nested-field")
            )
            .addSerializer(StructuredData.class, new StructuredDataJsonSerializer())
    );
  }
}
