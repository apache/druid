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

import com.google.inject.Injector;
import org.apache.druid.error.ExceptionMatcher;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.utils.RuntimeInfo;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class StartupInjectorBuilderTest
{
  @Test
  public void testEmpty()
  {
    Injector injector = new StartupInjectorBuilder().build();

    // Empty properties come along for free
    Properties props = injector.getInstance(Properties.class);
    assertNotNull(props);
    assertTrue(props.isEmpty());

    // Since we didn't configure this item, we get a new instance every time.
    assertNotSame(props, injector.getInstance(Properties.class));

    // Runtime info is available, though not configured, because Guice can create
    // one when requested. Our class, so marked singleton.
    assertNotNull(injector.getInstance(RuntimeInfo.class));
    assertSame(injector.getInstance(RuntimeInfo.class), injector.getInstance(RuntimeInfo.class));

    // The extension loader is available, again via implicit creation.
    // Since it is our class, we marked it as a lazy singleton.
    assertNotNull(injector.getInstance(ExtensionsLoader.class));
    assertSame(injector.getInstance(ExtensionsLoader.class), injector.getInstance(ExtensionsLoader.class));

    // Does have the basics. Sample one such entry.
    assertNotNull(injector.getInstance(DruidSecondaryModule.class));
    assertSame(injector.getInstance(DruidSecondaryModule.class), injector.getInstance(DruidSecondaryModule.class));
  }

  @Test
  public void testEmptyTestInjector()
  {
    Injector injector = new StartupInjectorBuilder().forTests().build();

    // Empty properties come along for free
    Properties props = injector.getInstance(Properties.class);
    assertNotNull(props);
    assertTrue(props.isEmpty());

    // Since we didn't configure this item, we get a new instance every time.
    assertNotSame(props, injector.getInstance(Properties.class));

    // Runtime info bound to null.
    assertNull(injector.getInstance(RuntimeInfo.class));

    // The extension loader bound to null.
    assertNull(injector.getInstance(ExtensionsLoader.class));

    // Does have the basics. Sample one such entry.
    assertNotNull(injector.getInstance(DruidSecondaryModule.class));
    assertSame(injector.getInstance(DruidSecondaryModule.class), injector.getInstance(DruidSecondaryModule.class));
  }

  @Test
  public void testEmptyProperties()
  {
    Injector injector = new StartupInjectorBuilder()
        .withEmptyProperties()
        .build();

    // Single empty properties instance
    Properties props = injector.getInstance(Properties.class);
    assertNotNull(props);
    assertTrue(props.isEmpty());

    // Since we didn't configure this item, we get a new instance every time.
    assertSame(props, injector.getInstance(Properties.class));
  }

  @Test
  public void testExplicitProperties()
  {
    Properties props = new Properties();
    props.put("foo", "bar");
    Injector injector = new StartupInjectorBuilder()
        .forTests()
        .withProperties(props)
        .build();

    // Returns explicit properties
    Properties propsInstance = injector.getInstance(Properties.class);
    assertSame(props, propsInstance);
  }

  @Test
  public void testExtensionsOption()
  {
    Properties props = new Properties();
    props.put(ExtensionsConfig.PROPERTY_BASE + ".directory", "bogus");
    props.put(ModulesConfig.PROPERTY_BASE + ".excludeList", "[\"excluded\"]");
    Injector injector = new StartupInjectorBuilder()
        .withExtensions()
        .withProperties(props)
        .build();

    // Extensions config is populated. (Can't tests extensions themselves.)
    assertEquals("bogus", injector.getInstance(ExtensionsConfig.class).getDirectory());
    assertEquals(Collections.singletonList("excluded"), injector.getInstance(ModulesConfig.class).getExcludeList());
  }

  // Can't test the server option here: there are no actual property files to read.

  @Test
  public void testValidator()
  {
    final Properties propsDefaultValueMode = new Properties();
    propsDefaultValueMode.put(StartupInjectorBuilder.NULL_HANDLING_CONFIG_STRING, "true");

    Throwable t = Assert.assertThrows(
        ISE.class,
        () -> new StartupInjectorBuilder().withExtensions()
                                          .withProperties(propsDefaultValueMode)
                                          .build()
    );
    Assert.assertEquals(
        StringUtils.format(
            "druid.generic.useDefaultValueForNull set to 'true', but has been removed, see https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null for details for how to migrate to SQL compliant behavior",
            StartupInjectorBuilder.getVersionString()
        ),
        t.getMessage()
    );

    final Properties propsNo3vl = new Properties();
    propsNo3vl.put(StartupInjectorBuilder.THREE_VALUE_LOGIC_CONFIG_STRING, "false");
    t = Assert.assertThrows(
        ISE.class,
        () -> new StartupInjectorBuilder().withExtensions()
                                          .withProperties(propsNo3vl)
                                          .build()
    );
    Assert.assertEquals(
        StringUtils.format(
            "druid.generic.useThreeValueLogicForNativeFilters set to 'false', but has been removed, see https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null for details for how to migrate to SQL compliant behavior",
            StartupInjectorBuilder.getVersionString()
        ),
        t.getMessage()
    );

    final Properties propsNonStrictBooleans = new Properties();
    propsNonStrictBooleans.put(ExpressionProcessingConfig.NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING, "false");

    t = Assert.assertThrows(
        ISE.class,
        () -> new StartupInjectorBuilder().withExtensions()
                                          .withProperties(propsNonStrictBooleans)
                                          .build()
    );
    Assert.assertEquals(
        StringUtils.format(
            "druid.expressions.useStrictBooleans set to 'false', but has been removed, see https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null for details for how to migrate to SQL compliant behavior",
            StartupInjectorBuilder.getVersionString()
        ),
        t.getMessage()
    );
  }

  @Test
  public void verifyInjectorBuild_withDeletedConfig_throwsException()
  {
    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.pool.parallelism",
        "10",
        "Config[druid.processing.merge.pool.parallelism] has been removed."
        + " Please use config[druid.processing.merge.parallelism] instead."
    );
    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.pool.awaitShutdownMillis",
        "1000",
        "Config[druid.processing.merge.pool.awaitShutdownMillis] has been removed."
        + " Please use config[druid.processing.merge.awaitShutdownMillis] instead."
    );
    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.pool.defaultMaxQueryParallelism",
        "100",
        "Config[druid.processing.merge.pool.defaultMaxQueryParallelism] has been removed."
        + " Please use config[druid.processing.merge.defaultMaxQueryParallelism] instead."
    );

    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.task.targetRunTimeMillis",
        "10",
        "Config[druid.processing.merge.task.targetRunTimeMillis] has been removed."
        + " Please use config[druid.processing.merge.targetRunTimeMillis] instead."
    );
    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.task.initialYieldNumRows",
        "1000",
        "Config[druid.processing.merge.task.initialYieldNumRows] has been removed."
        + " Please use config[druid.processing.merge.initialYieldNumRows] instead."
    );
    verifyInjectorBuild_withDeletedConfig_throwsException(
        "druid.processing.merge.task.smallBatchNumRows",
        "100",
        "Config[druid.processing.merge.task.smallBatchNumRows] has been removed."
        + " Please use config[druid.processing.merge.smallBatchNumRows] instead."
    );
  }

  private static void verifyInjectorBuild_withDeletedConfig_throwsException(
      String removedProperty,
      String dummyValue,
      String expectedMessage
  )
  {
    final Properties props = new Properties();
    props.setProperty(removedProperty, dummyValue);

    final StartupInjectorBuilder builder = new StartupInjectorBuilder().withExtensions().withProperties(props);
    MatcherAssert.assertThat(
        Assert.assertThrows(ISE.class, builder::build),
        ExceptionMatcher.of(ISE.class).expectMessageIs(expectedMessage)
    );
  }
}
