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

package org.apache.druid.query.lookup;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.guice.ExpressionModule;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.dimension.ExtractionDimensionSpec;
import org.apache.druid.query.filter.DimFilter;
import org.apache.druid.query.filter.SelectorDimFilter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.column.ValueType;
import org.apache.druid.segment.transform.ExpressionTransform;
import org.apache.druid.segment.virtual.ExpressionVirtualColumn;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class LookupSerdeModuleTest
{
  private Injector injector;
  private ObjectMapper objectMapper;

  @Before
  public void setUp()
  {
    final ImmutableList<DruidModule> modules = ImmutableList.of(
        new ExpressionModule(),
        new LookupSerdeModule()
    );

    injector = GuiceInjectors.makeStartupInjectorWithModules(modules);
    objectMapper = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    objectMapper.setInjectableValues(
        new InjectableValues.Std()
            .addValue(ExprMacroTable.class, injector.getInstance(ExprMacroTable.class))
            .addValue(
                LookupExtractorFactoryContainerProvider.class,
                injector.getInstance(LookupExtractorFactoryContainerProvider.class)
            )
    );
    modules.stream().flatMap(module -> module.getJacksonModules().stream()).forEach(objectMapper::registerModule);
  }

  @Test
  public void testExpressionVirtualColumnSerde() throws Exception
  {
    final ExpressionVirtualColumn virtualColumn = new ExpressionVirtualColumn(
        "v",
        "lookup(xxx, 'beep')",
        ValueType.STRING,
        injector.getInstance(ExprMacroTable.class)
    );

    Assert.assertEquals(
        virtualColumn,
        objectMapper.readValue(objectMapper.writeValueAsBytes(virtualColumn), VirtualColumn.class)
    );
  }

  @Test
  public void testExtractionDimensionSerde() throws Exception
  {
    final ExtractionDimensionSpec dimensionSpec = new ExtractionDimensionSpec(
        "xxx",
        "d",
        new RegisteredLookupExtractionFn(null, "beep", false, null, null, null)
    );

    Assert.assertEquals(
        dimensionSpec,
        objectMapper.readValue(objectMapper.writeValueAsBytes(dimensionSpec), DimensionSpec.class)
    );
  }

  @Test
  public void testExtractionFilterSere() throws Exception
  {
    final SelectorDimFilter filter = new SelectorDimFilter(
        "xxx",
        "d",
        new RegisteredLookupExtractionFn(null, "beep", false, null, null, null)
    );

    Assert.assertEquals(
        filter,
        objectMapper.readValue(objectMapper.writeValueAsBytes(filter), DimFilter.class)
    );
  }

  @Test
  public void testExpressionTransformSerde() throws Exception
  {
    final ExpressionTransform transform = new ExpressionTransform(
        "xxx",
        "lookup(xxx, 'beep')",
        injector.getInstance(ExprMacroTable.class)
    );

    Assert.assertEquals(
        transform,
        objectMapper.readValue(objectMapper.writeValueAsBytes(transform), ExpressionTransform.class)
    );
  }
}
