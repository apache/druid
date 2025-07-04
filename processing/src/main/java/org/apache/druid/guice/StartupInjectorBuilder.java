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
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.util.Providers;
import org.apache.druid.jackson.JacksonModule;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.math.expr.ExpressionProcessingConfig;
import org.apache.druid.math.expr.ExpressionProcessingModule;
import org.apache.druid.utils.RuntimeInfo;

import java.util.Arrays;
import java.util.Properties;

/**
 * Create the startup injector used to "prime" the modules for the
 * main injector.
 * <p>
 * Servers call the {@link #forServer()} method to configure server-style
 * properties and the server metrics. Servers must also add
 * {@code org.apache.druid.initialization.ExtensionsModule} which is
 * not visible here, and can't be added in the {@link #forServer()}
 * method.
 * <p>
 * Tests and clients must provide
 * properties via another mechanism.
 * <p>
 * If every test and client needs a module, it should be present here.
 */
public class StartupInjectorBuilder extends BaseInjectorBuilder<StartupInjectorBuilder>
{

  static final String NULL_HANDLING_CONFIG_STRING = "druid.generic.useDefaultValueForNull";
  static final String THREE_VALUE_LOGIC_CONFIG_STRING = "druid.generic.useThreeValueLogicForNativeFilters";

  public StartupInjectorBuilder()
  {
    add(
        new DruidGuiceExtensions(),
        new JacksonModule(),
        new ConfigModule(),
        new ExpressionProcessingModule(),
        binder -> binder.bind(DruidSecondaryModule.class),
        binder -> binder.bind(PropertiesValidator.class) // this gets properties injected, later call to validate checks
    );
  }

  @Override
  public Injector build()
  {
    Injector injector = super.build();
    injector.getInstance(PropertiesValidator.class).validate();
    return injector;
  }

  public StartupInjectorBuilder withProperties(Properties properties)
  {
    add(binder -> binder.bind(Properties.class).toInstance(properties));
    return this;
  }

  public StartupInjectorBuilder withEmptyProperties()
  {
    return withProperties(new Properties());
  }

  public StartupInjectorBuilder withExtensions()
  {
    add(new ExtensionsModule());
    return this;
  }

  public StartupInjectorBuilder forServer()
  {
    withExtensions();
    add(new PropertiesModule(Arrays.asList("common.runtime.properties", "runtime.properties")));
    return this;
  }

  /**
   * Configure the injector to not load server-only classes by binding those
   * classes to providers of null values. Avoids accidental dependencies of
   * test code on classes not intended for classes by preventing Guice from
   * helpfully providing implicit instances.
   */
  public StartupInjectorBuilder forTests()
  {
    add(binder -> {
      binder.bind(ExtensionsLoader.class).toProvider(Providers.of(null));
      binder.bind(RuntimeInfo.class).toProvider(Providers.of(null));
    });
    return this;
  }

  /**
   * Centralized validation of runtime.properties to allow checking for configuration which has been removed and
   * alerting or failing fast as needed.
   */
  private static final class PropertiesValidator
  {
    private final Properties properties;

    @Inject
    public PropertiesValidator(Properties properties)
    {
      this.properties = properties;
    }

    public void validate()
    {
      final boolean defaultValueMode = Boolean.parseBoolean(
          properties.getProperty(NULL_HANDLING_CONFIG_STRING, "false")
      );
      if (defaultValueMode) {
        final String docsLink = StringUtils.format("https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null", getVersionString());
        throw new ISE(
            "%s set to 'true', but has been removed, see %s for details for how to migrate to SQL compliant behavior",
            NULL_HANDLING_CONFIG_STRING,
            docsLink
        );
      }

      final boolean no3vl = !Boolean.parseBoolean(
          properties.getProperty(THREE_VALUE_LOGIC_CONFIG_STRING, "true")
      );
      if (no3vl) {
        final String docsLink = StringUtils.format("https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null", getVersionString());
        throw new ISE(
            "%s set to 'false', but has been removed, see %s for details for how to migrate to SQL compliant behavior",
            THREE_VALUE_LOGIC_CONFIG_STRING,
            docsLink
        );
      }

      final boolean nonStrictExpressions = !Boolean.parseBoolean(
          properties.getProperty(ExpressionProcessingConfig.NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING, "true")
      );
      if (nonStrictExpressions) {
        final String docsLink = StringUtils.format("https://druid.apache.org/docs/%s/release-info/migr-ansi-sql-null", getVersionString());
        throw new ISE(
            "%s set to 'false', but has been removed, see %s for details for how to migrate to SQL compliant behavior",
            ExpressionProcessingConfig.NULL_HANDLING_LEGACY_LOGICAL_OPS_STRING,
            docsLink
        );
      }

      validateRemovedProcessingConfigs();
    }

    private void validateRemovedProcessingConfigs()
    {
      checkDeletedConfigAndThrow(
          "druid.processing.merge.task.initialYieldNumRows",
          "druid.processing.merge.initialYieldNumRows"
      );
      checkDeletedConfigAndThrow(
          "druid.processing.merge.task.targetRunTimeMillis",
          "druid.processing.merge.targetRunTimeMillis"
      );
      checkDeletedConfigAndThrow(
          "druid.processing.merge.task.smallBatchNumRows",
          "druid.processing.merge.smallBatchNumRows"
      );

      checkDeletedConfigAndThrow(
          "druid.processing.merge.pool.awaitShutdownMillis",
          "druid.processing.merge.awaitShutdownMillis"
      );
      checkDeletedConfigAndThrow(
          "druid.processing.merge.pool.parallelism",
          "druid.processing.merge.parallelism"
      );
      checkDeletedConfigAndThrow(
          "druid.processing.merge.pool.defaultMaxQueryParallelism",
          "druid.processing.merge.defaultMaxQueryParallelism"
      );
    }

    /**
     * Checks if a deleted config is present in the properties and throws an ISE.
     */
    private void checkDeletedConfigAndThrow(String deletedConfigName, String replaceConfigName)
    {
      if (properties.getProperty(deletedConfigName) != null) {
        throw new ISE(
            "Config[%s] has been removed. Please use config[%s] instead.",
            deletedConfigName,
            replaceConfigName
        );
      }
    }
  }

  @VisibleForTesting
  public static String getVersionString()
  {
    final String version = StartupInjectorBuilder.class.getPackage().getImplementationVersion();
    if (version == null || version.contains("SNAPSHOT")) {
      return "latest";
    }
    return version;
  }
}
