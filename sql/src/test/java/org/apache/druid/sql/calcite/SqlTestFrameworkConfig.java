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

import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.rules.ExternalResource;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

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
  class ClassRule extends ExternalResource
  {

    Map<SqlTestFrameworkConfig, ConfigurationInstance> configMap = new HashMap<>();

    public MethodRule methodRule(BaseCalciteQueryTest testHost)
    {
      return new MethodRule(this, testHost);
    }

    @Override
    protected void after()
    {
      for (ConfigurationInstance f : configMap.values()) {
        f.close();
      }
      configMap.clear();
    }
  }

  /**
   * @see {@link SqlTestFrameworkConfig}
   */
  class MethodRule extends ExternalResource
  {
    private SqlTestFrameworkConfig config;
    private ClassRule classRule;
    private QueryComponentSupplier testHost;
    private Description description;

    public MethodRule(ClassRule classRule, QueryComponentSupplier testHost)
    {
      this.classRule = classRule;
      this.testHost = testHost;
    }

    @SqlTestFrameworkConfig
    public SqlTestFrameworkConfig defaultConfig()
    {
      try {
        SqlTestFrameworkConfig annotation = MethodRule.class
            .getMethod("defaultConfig")
            .getAnnotation(SqlTestFrameworkConfig.class);
        return annotation;
      }
      catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
      this.description = description;
      config = description.getAnnotation(SqlTestFrameworkConfig.class);
      if (config == null) {
        config = defaultConfig();
      }
      return base;
    }

    public SqlTestFramework get()
    {
      return getConfigurationInstance().framework;
    }

    public Description getDescription()
    {
      return description;
    }

    private ConfigurationInstance getConfigurationInstance()
    {
      return classRule.configMap.computeIfAbsent(config, this::buildConfiguration);
    }

    ConfigurationInstance buildConfiguration(SqlTestFrameworkConfig config)
    {
      return new ConfigurationInstance(config, testHost);
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
