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
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class StringMultiValueHandlingModuleTest
{
  @Test
  public void testDefaults()
  {
    final Properties props = new Properties();
    final Injector gadget = makeInjector(props);

    gadget.getInstance(StringMultiValueHandlingModule.SideEffectHandlerRegisterer.class);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        StringMultiValueHandlingModule.getConfiguredStringMultiValueHandlingMode()
    );
  }

  @Test
  public void testOverrides()
  {
    final Properties props = new Properties();
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "ARRAY");
    final Injector gadget = makeInjector(props);

    gadget.getInstance(StringMultiValueHandlingModule.SideEffectHandlerRegisterer.class);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.ARRAY,
        StringMultiValueHandlingModule.getConfiguredStringMultiValueHandlingMode()
    );
  }


  @Test
  public void testOverridesCaseIsensitiveMode()
  {
    final Properties props = new Properties();
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "sorted_array");
    final Injector gadget = makeInjector(props);

    gadget.getInstance(StringMultiValueHandlingModule.SideEffectHandlerRegisterer.class);

    Assert.assertEquals(
        DimensionSchema.MultiValueHandling.SORTED_ARRAY,
        StringMultiValueHandlingModule.getConfiguredStringMultiValueHandlingMode()
    );
  }

  @Test
  public void testInvalidMode()
  {
    final Properties props = new Properties();
    props.setProperty("druid.indexing.formats.stringMultiValueHandlingMode", "boo");
    final Injector gadget = makeInjector(props);

    final Exception exception = Assert.assertThrows(
        Exception.class,
        () -> gadget.getInstance(StringMultiValueHandlingModule.SideEffectHandlerRegisterer.class)
    );
    Assert.assertTrue(exception.getMessage().contains(
        "Invalid value[boo] specified for 'druid.indexing.formats.stringMultiValueHandlingMode'."
        + " Supported values are [[SORTED_ARRAY, SORTED_SET, ARRAY]]."
    ));
  }

  private Injector makeInjector(final Properties props)
  {
    final StartupInjectorBuilder bob = new StartupInjectorBuilder().forTests().withProperties(props);

    bob.addAll(
        ImmutableList.of(
            binder -> {
              JsonConfigProvider.bind(binder, "druid.indexing.formats", DefaultColumnFormatConfig.class);
            },
            new StringMultiValueHandlingModule()
        )
    );

    return bob.build();
  }
}
