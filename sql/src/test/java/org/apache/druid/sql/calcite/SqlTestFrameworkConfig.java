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
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  public @interface NumMergeBuffers {
    int value() default 0;
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  public @interface MinTopNThreshold {
    int value() default 0;
  }

  /**
   * Declares which {@link QueryComponentSupplier} must be used for the class.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.TYPE}) @interface SqlTestFrameWorkModule
  {
    Class<? extends QueryComponentSupplier> value();
  }

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

    public SqlTestFrameworkConfigInstance(SqlTestFrameworkConfig ...annotations)
    {
      try {
        numMergeBuffers = getValue(annotations, SqlTestFrameworkConfig.class.getMethod("numMergeBuffers"));
        minTopNThreshold = getValue(annotations, SqlTestFrameworkConfig.class.getMethod("minTopNThreshold"));
        resultCache = getValue(annotations, SqlTestFrameworkConfig.class.getMethod("resultCache"));
        supplier = getValue(annotations, SqlTestFrameworkConfig.class.getMethod("supplier"));
      }
      catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    public SqlTestFrameworkConfigInstance(Annotation[] annotations)
    {
      try {
        numMergeBuffers = getValue2(annotations, NumMergeBuffers.class.getMethod("value"));
        minTopNThreshold = getValue2(annotations, MinTopNThreshold.class.getMethod("value"));
//        resultCache = getValue2(annotations, SqlTestFrameworkConfig.class.getMethod("resultCache"));
//        supplier = getValue2(annotations, SqlTestFrameworkConfig.class.getMethod("supplier"));
      }
      catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
      this.resultCache = null;
      this.supplier = null;
    }

    private <T> T getValue(SqlTestFrameworkConfig[] annotations, Method method) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
      T  value = (T) method.getDefaultValue();
      Class<? extends Annotation> at = annotations[0].annotationType();
      Class<? extends Annotation> at2 = annotations[0].getClass();
        Class<SqlTestFrameworkConfig> ct = SqlTestFrameworkConfig.class;
      for (SqlTestFrameworkConfig annotation : annotations) {
        value = (T) method.invoke(annotation);
      }
      return value;

    }
    private <T> T getValue2(Annotation[] annotations, Method method) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException
    {
      T  value = (T) method.getDefaultValue();
//      Class<? extends Annotation> at = annotations[0].annotationType();
//      Class<? extends Annotation> at2 = annotations[0].getClass();
//        Class<SqlTestFrameworkConfig> ct = SqlTestFrameworkConfig.class;
      Class<?> declaringClass = method.getDeclaringClass();
      for (Annotation annotation : annotations) {
        if (declaringClass.isInstance(annotation)) {
          value = (T) method.invoke(annotation);
        }
      }
      return value;

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
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper
    ) throws Exception
    {
      ConfigurationInstance ret = configMap.get(config);
      if (!configMap.containsKey(config)) {
        ret = new ConfigurationInstance(config, new TempDirProducer("druid-test"), queryComponentSupplierWrapper);
        configMap.put(config, ret);
      }
      return ret;
    }

    @Override
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
    private Function<TempDirProducer, QueryComponentSupplier> testHostSupplier;
    private Method method;

    private void setComponentSupplier(Class<? extends QueryComponentSupplier> value) throws NoSuchMethodException
    {
      Constructor<? extends QueryComponentSupplier> constructor = value.getConstructor(TempDirProducer.class);
      testHostSupplier = f -> {
        try {
          return constructor.newInstance(f);
        }
        catch (Exception e) {
          throw new RE(e, "Unable to create QueryComponentSupplier");
        }
      };
    }

    private SqlTestFrameworkConfig.SqlTestFrameWorkModule getModuleAnnotationFor(Class<?> testClass)
    {
      SqlTestFrameworkConfig.SqlTestFrameWorkModule annotation = testClass.getAnnotation(SqlTestFrameworkConfig.SqlTestFrameWorkModule.class);
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
      List<SqlTestFrameworkConfig> annotations = new ArrayList<>();
      annotations.add(method.getAnnotation(SqlTestFrameworkConfig.class));

      Class<?> clz = method.getDeclaringClass();
      while (clz != null) {
        annotations.add(clz.getAnnotation(SqlTestFrameworkConfig.class));
        clz = clz.getSuperclass();
      }
      annotations.removeIf(v -> v == null);

      SqlTestFrameworkConfig[] array = annotations.toArray(new SqlTestFrameworkConfig[] {});
      config = new SqlTestFrameworkConfigInstance(array);
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
      return configStore.getConfigurationInstance(config, x -> x).framework;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationType)
    {
      return method.getAnnotation(annotationType);
    }

    public String testName()
    {
      return method.getName();
    }
  }

  public class ConfigurationInstance
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
        TempDirProducer tempDirProducer,
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper
    ) throws Exception
    {
      this(config, queryComponentSupplierWrapper.apply(makeQueryComponentSupplier(config.supplier, tempDirProducer)));
    }

    private static QueryComponentSupplier makeQueryComponentSupplier(
        Class<? extends QueryComponentSupplier> supplierClazz,
        TempDirProducer tempDirProducer) throws Exception
    {
      Constructor<? extends QueryComponentSupplier> constructor = supplierClazz.getConstructor(TempDirProducer.class);
      return constructor.newInstance(tempDirProducer);
    }

    public void close()
    {
      framework.close();
    }
  }
}
