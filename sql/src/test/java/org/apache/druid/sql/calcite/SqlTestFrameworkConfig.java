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

import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.function.Function;

/**
 * Annotation to specify desired framework settings.
 *
 * This class provides junit rule facilities to build the framework accordingly
 * to the annotation. These rules also cache the previously created frameworks.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.TYPE})
public @interface SqlTestFrameworkConfig
{
  int numMergeBuffers() default 0;

  int minTopNThreshold() default TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD;

  ResultCacheMode resultCache() default ResultCacheMode.DISABLED;

  Class<? extends QueryComponentSupplier> supplier() default StandardComponentSupplier.class;

  /**
   * Non-annotation version of {@link SqlTestFrameworkConfig}.
   *
   * Makes it less convoluted to work with configurations created at runtime.
   */
  class SqlTestFrameworkConfigInstance
  {
    public final int numMergeBuffers;
    public final int minTopNThreshold;
    public final ResultCacheMode resultCache;
    public final Class<? extends QueryComponentSupplier> supplier;

    public SqlTestFrameworkConfigInstance(SqlTestFrameworkConfig annotation)
    {
      numMergeBuffers = annotation.numMergeBuffers();
      minTopNThreshold = annotation.minTopNThreshold();
      resultCache = annotation.resultCache();
      supplier = annotation.supplier();
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(minTopNThreshold, numMergeBuffers, resultCache, supplier);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (obj == null || getClass() != obj.getClass()) {
        return false;
      }
      SqlTestFrameworkConfigInstance other = (SqlTestFrameworkConfigInstance) obj;
      return minTopNThreshold == other.minTopNThreshold
          && numMergeBuffers == other.numMergeBuffers
          && resultCache == other.resultCache
          && supplier == other.supplier;
    }
  }

  class SqlTestFrameworkConfigStore implements Closeable
  {
    Map<SqlTestFrameworkConfigInstance, ConfigurationInstance> configMap = new HashMap<>();

    public ConfigurationInstance getConfigurationInstance(
        SqlTestFrameworkConfigInstance config,
        Callable<File> tempDirProducer,
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper
    ) throws Exception
    {
      ConfigurationInstance ret = configMap.get(config);
      if (!configMap.containsKey(config)) {
        ret = new ConfigurationInstance(config, tempDirProducer, queryComponentSupplierWrapper);
        configMap.put(config, ret);
      }
      return ret;
    }

    public void close()
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
  class Rule implements AfterAllCallback, BeforeEachCallback
  {
    SqlTestFrameworkConfigStore configStore = new SqlTestFrameworkConfigStore();
    private SqlTestFrameworkConfigInstance config;
    private Function<File, QueryComponentSupplier> testHostSupplier;
    private Method method;

    private void setComponentSupplier(Class<? extends QueryComponentSupplier> value) throws NoSuchMethodException
    {
      Constructor<? extends QueryComponentSupplier> constructor = value.getConstructor(File.class);
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
      configStore.close();
    }

    @Override
    public void beforeEach(ExtensionContext context) throws NoSuchMethodException
    {
      setConfig(context);
    }

    private void setConfig(ExtensionContext context) throws NoSuchMethodException
    {
      method = context.getTestMethod().get();
      SqlTestFrameworkConfig annotation = method.getAnnotation(SqlTestFrameworkConfig.class);
      if (annotation == null) {
        annotation = defaultConfig();
      }
      config = new SqlTestFrameworkConfigInstance(annotation);
      setComponentSupplier(config.supplier);


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

    public SqlTestFrameworkConfigInstance getConfig()
    {
      return config;
    }

    public SqlTestFramework get() throws Exception
    {
      return configStore.getConfigurationInstance(config, () -> createTempFolder("druid-test"), x -> x).framework;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationType)
    {
      return method.getAnnotation(annotationType);
    }

    public String testName()
    {
      return method.getName();
    }

    protected File createTempFolder(String prefix)
    {
      File tempDir = FileUtils.createTempDir(prefix);
      Runtime.getRuntime().addShutdownHook(new Thread()
      {
        @Override
        public void run()
        {
          try {
            FileUtils.deleteDirectory(tempDir);
          }
          catch (IOException ex) {
            ex.printStackTrace();
          }
        }
      });
      return tempDir;
    }
  }

  class ConfigurationInstance
  {
    public SqlTestFramework framework;

    ConfigurationInstance(SqlTestFrameworkConfigInstance config, QueryComponentSupplier testHost)
    {

      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .catalogResolver(testHost.createCatalogResolver())
          .minTopNThreshold(config.minTopNThreshold)
          .mergeBufferCount(config.numMergeBuffers)
          .withOverrideModule(config.resultCache.makeModule());
      framework = builder.build();
    }

    public ConfigurationInstance(
        SqlTestFrameworkConfigInstance config,
        Callable<File> tempDirProducer,
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper
    ) throws Exception
    {
      this(config, queryComponentSupplierWrapper.apply(makeQueryComponentSupplier(config.supplier, tempDirProducer)));
    }

    private static QueryComponentSupplier makeQueryComponentSupplier(
        Class<? extends QueryComponentSupplier> supplierClazz,
        Callable<File> tempDirProducer) throws Exception
    {
      Constructor<? extends QueryComponentSupplier> constructor = supplierClazz.getConstructor(File.class);
      return constructor.newInstance(tempDirProducer.call());
    }

    public void close()
    {
      framework.close();
    }
  }
}
