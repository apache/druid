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

package org.apache.druid.sql.calcite;

import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Annotation to specify desired framework settings.
 *
 * This class provides junit rule facilities to build the framework accordingly
 * to the annotation. These rules also cache the previously created frameworks.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SqlTestFrameworkConfig
{
  int numMergeBuffers() default 0;

  int minTopNThreshold() default TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  ResultCacheMode resultCache() default ResultCacheMode.DISABLED;



  /**
   * @see {@link SqlTestFrameworkConfig}
   */
  class Rule implements AfterAllCallback, BeforeEachCallback, BeforeAllCallback
  {
    Map<SqlTestFrameworkConfig, ConfigurationInstance> configMap = new HashMap<>();
    private SqlTestFrameworkConfig config;
    private Function<TempDirProducer, QueryComponentSupplier> testHostSupplier;
    private Method method;

    @Override
    public void beforeAll(ExtensionContext context) throws Exception
    {
      Class<?> testClass = context.getTestClass().get();
      SqlTestFramework.SqlTestFrameWorkModule moduleAnnotation = getModuleAnnotationFor(testClass);
      Constructor<? extends QueryComponentSupplier> constructor = moduleAnnotation.value().getConstructor(TempDirProducer.class);
      testHostSupplier = f -> {
        try {
          return constructor.newInstance(f);
        }
        catch (Exception e) {
          throw new RE(e, "Unable to create QueryComponentSupplier");
        }
      };
    }

    private SqlTestFramework.SqlTestFrameWorkModule getModuleAnnotationFor(Class<?> testClass)
    {
      SqlTestFramework.SqlTestFrameWorkModule annotation = testClass.getAnnotation(SqlTestFramework.SqlTestFrameWorkModule.class);
      if (annotation == null) {
        if (testClass.getSuperclass() == null) {
          throw new RE("Can't get QueryComponentSupplier for testclass!");
        }
        return getModuleAnnotationFor(testClass.getSuperclass());
      }
      return annotation;
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
      for (ConfigurationInstance f : configMap.values()) {
        f.close();
      }
      configMap.clear();
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
      method = context.getTestMethod().get();
      setConfig(method.getAnnotation(SqlTestFrameworkConfig.class));
    }

    @SqlTestFrameworkConfig
    public SqlTestFrameworkConfig defaultConfig()
    {
      try {
        SqlTestFrameworkConfig annotation = getClass()
            .getMethod("defaultConfig")
            .getAnnotation(SqlTestFrameworkConfig.class);
        return annotation;
      }
      catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    public void setConfig(SqlTestFrameworkConfig annotation)
    {
      config = annotation;
      if (config == null) {
        config = defaultConfig();
      }
    }

    public SqlTestFramework get()
    {
      return getConfigurationInstance().framework;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationType)
    {
      return method.getAnnotation(annotationType);
    }

    private ConfigurationInstance getConfigurationInstance()
    {
      return configMap.computeIfAbsent(config, this::buildConfiguration);
    }

    ConfigurationInstance buildConfiguration(SqlTestFrameworkConfig config)
    {
      return new ConfigurationInstance(config, testHostSupplier.apply(new TempDirProducer("druid-test")));
    }
  }

  class ConfigurationInstance
  {
    public SqlTestFramework framework;

    ConfigurationInstance(SqlTestFrameworkConfig config, QueryComponentSupplier testHost)
    {
      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .catalogResolver(testHost.createCatalogResolver())
          .minTopNThreshold(config.minTopNThreshold())
          .mergeBufferCount(config.numMergeBuffers())
          .withOverrideModule(config.resultCache().makeModule());
      framework = builder.build();
    }

    public void close()
    {
      framework.close();
    }
  }
}
