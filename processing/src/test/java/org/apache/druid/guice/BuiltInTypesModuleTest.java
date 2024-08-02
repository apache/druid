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

import com.google.common.collect.ImmutableList;
import com.google.inject.Injector;
import org.apache.druid.data.input.impl.DimensionSchema;
import org.apache.druid.segment.DefaultColumnFormatConfig;
import org.apache.druid.segment.DimensionHandlerProvider;
import org.apache.druid.segment.DimensionHandlerUtils;
import org.apache.druid.segment.NestedCommonFormatColumnHandler;
import org.apache.druid.segment.NestedDataColumnHandlerV4;
import org.apache.druid.segment.nested.NestedDataComplexTypeSerde;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.annotation.Nullable;
import java.util.Properties;

public class BuiltInTypesModuleTest
{
  @Nullable
  private static DimensionHandlerProvider DEFAULT_HANDLER_PROVIDER;

  @BeforeClass
  public static void setup()
  {
    DEFAULT_HANDLER_PROVIDER = DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.get(
        NestedDataComplexTypeSerde.TYPE_NAME
    );
    DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.remove(NestedDataComplexTypeSerde.TYPE_NAME);
  }
  @AfterClass
  public static void teardown()
  {
    if (DEFAULT_HANDLER_PROVIDER == null) {
      DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.remove(NestedDataComplexTypeSerde.TYPE_NAME);
    } else {
      DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.put(
          NestedDataComplexTypeSerde.TYPE_NAME,
          DEFAULT_HANDLER_PROVIDER
      );
    }
  }

  @Test
  public void testDefaults()
  {
    DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.remove(NestedDataComplexTypeSerde.TYPE_NAME);
    Properties props = new Properties();
    Injector gadget = makeInjector(props);

    // side effects
    gadget.getInstance(BuiltInTypesModule.SideEffectRegisterer.class);

    DimensionHandlerProvider provider = DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.get(
        NestedDataComplexTypeSerde.TYPE_NAME
    );
    Assert.assertTrue(provider.get("test") instanceof NestedCommonFormatColumnHandler);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        BuiltInTypesModule.getStringMultiValueHandlingMode()
    );
  }

  @Test
  public void testOverride()
  {
    DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.remove(NestedDataComplexTypeSerde.TYPE_NAME);
    Properties props = new Properties();
    props.setProperty("druid.indexing.formats.nestedColumnFormatVersion", "4");
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "sorted_array");
    Injector gadget = makeInjector(props);

    // side effects
    gadget.getInstance(BuiltInTypesModule.SideEffectRegisterer.class);

    DimensionHandlerProvider provider = DimensionHandlerUtils.DIMENSION_HANDLER_PROVIDERS.get(
        NestedDataComplexTypeSerde.TYPE_NAME
    );
    Assert.assertTrue(provider.get("test") instanceof NestedDataColumnHandlerV4);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        BuiltInTypesModule.getStringMultiValueHandlingMode()
    );
  }

  @Test
  public void testOverrideMultiValueHandlingModeCaseInsensitive()
  {
    final Properties props = new Properties();
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "ARRAY");
    final Injector gadget = makeInjector(props);

    gadget.getInstance(BuiltInTypesModule.SideEffectRegisterer.class);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.ARRAY,
        BuiltInTypesModule.getStringMultiValueHandlingMode()
    );
  }

  @Test
  public void testInvalidMultiValueHandlingMode()
  {
    final Properties props = new Properties();
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "boo");
    final Injector gadget = makeInjector(props);

    final Exception exception = Assert.assertThrows(
        Exception.class,
        () -> gadget.getInstance(BuiltInTypesModule.SideEffectRegisterer.class)
    );
    Assert.assertTrue(exception.getMessage().contains(
        "Invalid value[boo] specified for 'druid.indexing.formats.stringMultiValueHandlingMode'."
        + " Supported values are [[SORTED_ARRAY, SORTED_SET, ARRAY]]."
    ));
  }

  private Injector makeInjector(Properties props)
  {

    StartupInjectorBuilder bob = new StartupInjectorBuilder().forTests().withProperties(props);

    bob.addAll(
        ImmutableList.of(
            binder -> {
              JsonConfigProvider.bind(binder, "druid.indexing.formats", DefaultColumnFormatConfig.class);
            },
            new BuiltInTypesModule()
        )
    );

    return bob.build();
  }
}
