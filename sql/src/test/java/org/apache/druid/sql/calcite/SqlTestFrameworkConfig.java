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

import com.google.inject.Module;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.MethodRule.ConfigurationInstance;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.rules.ExternalResource;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.HashMap;
import java.util.Map;

/**
 * Annotation to specify desired framework settings.
 *
 * This class provides junit rule facilities to build the framework accordingly to the annotation.
 * These rules also cache the previously created frameworks.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD})
public @interface SqlTestFrameworkConfig
{
  int numMergeBuffers() default 0;

  int minTopNThreshold() default TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  ResultCacheMode resultCache() default ResultCacheMode.DISABLED;

  static enum ResultCacheMode {
    DISABLED,
    ENABLE_ISOLATED;

    Module makeModule()
    {
      return new CacheTestHelperModule(this);
    }

    public boolean isPopulateResultLevelCache()
    {
      return this != DISABLED;
    }

    public boolean isUseResultLevelCache()
    {
      return this != DISABLED;
    }
  }

  /**
   * @see {@link SqlTestFrameworkConfig}
   */
  class ClassRule extends ExternalResource
  {

    Map<SqlTestFrameworkConfig, ConfigurationInstance> configMap = new HashMap<>();

    public MethodRule methodRule(BaseCalciteQueryTest testHost, TemporaryFolder temporaryFolder)
    {
      return new MethodRule(this, testHost, temporaryFolder);
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
  class MethodRule implements TestRule
  {
    private SqlTestFrameworkConfig config;
    private ClassRule classRule;
    private QueryComponentSupplier testHost;
    private TemporaryFolder temporaryFolder;

    public MethodRule(ClassRule classRule, QueryComponentSupplier testHost, TemporaryFolder temporaryFolder)
    {
      this.classRule = classRule;
      this.testHost = testHost;
      this.temporaryFolder = temporaryFolder;
    }

    @SqlTestFrameworkConfig
    public SqlTestFrameworkConfig defaultConfig()
    {
      try {
        return getClass()
            .getMethod("defaultConfig")
            .getAnnotation(SqlTestFrameworkConfig.class);
      }
      catch (NoSuchMethodException | SecurityException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public Statement apply(Statement base, Description description)
    {
      config = description.getAnnotation(SqlTestFrameworkConfig.class);
      if (config == null) {
        config = defaultConfig();
      }
//      SqlTestFramework framework = get();


      return base;
    }

    public SqlTestFramework get()
    {
      return getCurrentConfigInstance().framework;
    }

    public QueryComponentSupplier getQueryComponentSupplier()
    {
      return getCurrentConfigInstance().baseComponentSupplier;
    }

    private ConfigurationInstance getCurrentConfigInstance()
    {
      return classRule.configMap.computeIfAbsent(config, ConfigurationInstance::new);
    }

    class ConfigurationInstance {

      public SqlTestFramework framework;
      private StandardComponentSupplier baseComponentSupplier;

      ConfigurationInstance(SqlTestFrameworkConfig config)
      {
        try {
          baseComponentSupplier = new StandardComponentSupplier(
              temporaryFolder.newFolder()
          );
          SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
              .minTopNThreshold(config.minTopNThreshold())
              .mergeBufferCount(config.numMergeBuffers())
              .withExtraModule(config.resultCache().makeModule());
          framework = builder.build();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      public void close()
      {
        framework.close();
      }
    }

  }

}
