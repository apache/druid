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

import com.google.api.client.util.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.RE;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.reflections.Reflections;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Annotation to specify desired framework settings.
 *
 * This class provides junit rule facilities to build the framework accordingly
 * to the annotation. These rules also cache the previously created frameworks.
 */
public interface SqlTestFrameworkConfig
{
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @NumMergeBuffers(0)
  public @interface NumMergeBuffers {
    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @MinTopNThreshold(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
  public @interface MinTopNThreshold {
    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @ResultCache(ResultCacheMode.DISABLED)
  public @interface ResultCache
  {
    ResultCacheMode value();
  }

  /**
   * Declares which {@link QueryComponentSupplier} must be used for the class.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @SqlTestFrameWorkModule(StandardComponentSupplier.class)
  public @interface SqlTestFrameWorkModule
  {
    Class<? extends QueryComponentSupplier> value();
  }

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

    public SqlTestFrameworkConfigInstance(List<Annotation> annotations)
    {
      try {
        numMergeBuffers = getValue(annotations, NumMergeBuffers.class);
        minTopNThreshold = getValue(annotations, MinTopNThreshold.class);
        resultCache = getValue(annotations, ResultCache.class);
        supplier = getValue(annotations, SqlTestFrameWorkModule.class);
      }
      catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
          | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    public SqlTestFrameworkConfigInstance(Map<String, String> queryParams)
    {
      try {
      numMergeBuffers = getValue2(queryParams, NumMergeBuffers.class);
      minTopNThreshold = getValue2(queryParams, MinTopNThreshold.class);
      resultCache = getValue2(queryParams, ResultCache.class);
      supplier = getValue2(queryParams, SqlTestFrameWorkModule.class);
    }
    catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
    }

    private <T> T getValue2(Map<String, String> map, Class<? extends Annotation> annotationClass) throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException
    {
      String value = map.get(annotationClass.getSimpleName());
      if(value == null ) {
        return defaultValue(annotationClass);
      }
      Class<?> type = annotationClass.getMethod("value").getReturnType();

      if (type == int.class) {
        return (T) Integer.valueOf(value);
      }
      if (type == Class.class) {

        Object clazz = getQueryComponentSupplierForName(value);
        if (clazz != null) {
          return (T) clazz;
        }

      }
      throw new RuntimeException("don't know how to handle conversion to " + type);
    }

    private Object getQueryComponentSupplierForName(String name)
    {
      Set<Class<? extends QueryComponentSupplier>> subTypes = new Reflections("org.apache.druid")
          .getSubTypesOf(QueryComponentSupplier.class);
      Set<String> knownNames = new HashSet<String>();

      for (Class<? extends QueryComponentSupplier> cl : subTypes) {
        if (cl.getSimpleName().equals(name)) {
          return cl;
        }
        knownNames.add(cl.getSimpleName());
      }
      throw new IAE("supplier [%s] is not known; known are [%s]", name, knownNames);
    }


    private <T> T getValue(List<Annotation> annotations, Class<? extends Annotation> annotationClass)
        throws IllegalAccessException, IllegalArgumentException, InvocationTargetException, NoSuchMethodException,
        SecurityException
    {
      Method method = annotationClass.getMethod("value");
      for (Annotation annotation : annotations) {
        if (annotationClass.isInstance(annotation)) {
          return (T) method.invoke(annotation);
        }
      }
      return defaultValue(annotationClass);
    }

    private <T> T defaultValue(Class<? extends Annotation> annotationClass)
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException
    {
      Method method = annotationClass.getMethod("value");
      Annotation annotation = annotationClass.getAnnotation(annotationClass);
      Preconditions.checkNotNull(
          annotation,
          String.format("Annotation class [%s] must be annotated with itself to set default value", annotationClass)
      );
      return (T) method.invoke(annotation);
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
    private Method method;

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
      List<Annotation> annotations = collectAnnotations(method);
      config = new SqlTestFrameworkConfigInstance(annotations);
    }

    private List<Annotation> collectAnnotations(Method method)
    {
      List<Annotation> annotations = new ArrayList<>();

      annotations.addAll(List.of(method.getAnnotations()));

      Class<?> clz = method.getDeclaringClass();
      while (clz != null) {
        annotations.addAll(List.of(clz.getAnnotations()));
        clz = clz.getSuperclass();
      }
      annotations.removeIf(
          annotation -> annotation.getClass().getInterfaces()[0].getDeclaringClass() != SqlTestFrameworkConfig.class
      );

      return annotations;
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
