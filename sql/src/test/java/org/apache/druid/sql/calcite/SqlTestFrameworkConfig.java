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

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Specifies current framework settings.
 *
 * Intended usage from tests is via the annotations:
 *   @SqlTestFrameworkConfig.MinTopNThreshold(33)
 *
 * In case of annotations used; it picks up all annotations from:
 *  * the method
 *  * its enclosing class and its parents
 * if none contains a specific setting the default is being taken.
 *
 * All configurable setting should have:
 *   * an annotation with `value` with the desired type
 *   * the annotation itself should be annotated with itslef to set the default value
 *   * a field should be added to the main config class
 */
public class SqlTestFrameworkConfig
{
  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @NumMergeBuffers(0)
  public @interface NumMergeBuffers
  {
    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @MinTopNThreshold(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
  public @interface MinTopNThreshold
  {
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
  @ComponentSupplier(StandardComponentSupplier.class)
  public @interface ComponentSupplier
  {
    Class<? extends QueryComponentSupplier> value();
  }

  public final int numMergeBuffers;
  public final int minTopNThreshold;
  public final ResultCacheMode resultCache;
  public final Class<? extends QueryComponentSupplier> componentSupplier;

  public SqlTestFrameworkConfig(List<Annotation> annotations)
  {
    try {
      numMergeBuffers = getValueFromAnnotation(annotations, NumMergeBuffers.class);
      minTopNThreshold = getValueFromAnnotation(annotations, MinTopNThreshold.class);
      resultCache = getValueFromAnnotation(annotations, ResultCache.class);
      componentSupplier = getValueFromAnnotation(annotations, ComponentSupplier.class);
    }
    catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  public SqlTestFrameworkConfig(Map<String, String> queryParams)
  {
    try {
      numMergeBuffers = getValueFromMap(queryParams, NumMergeBuffers.class);
      minTopNThreshold = getValueFromMap(queryParams, MinTopNThreshold.class);
      resultCache = getValueFromMap(queryParams, ResultCache.class);
      componentSupplier = getValueFromMap(queryParams, ComponentSupplier.class);
    }
    catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException
        | InvocationTargetException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  private <T> T getValueFromMap(Map<String, String> map, Class<? extends Annotation> annotationClass)
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException
  {
    String value = map.get(annotationClass.getSimpleName());
    if (value == null) {
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
    throw new IAE("ComponentSupplier [%s] is not known; known ones are [%s]", name, knownNames);
  }

  @SuppressWarnings("unchecked")
  private <T> T getValueFromAnnotation(List<Annotation> annotations, Class<? extends Annotation> annotationClass)
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

  @SuppressWarnings("unchecked")
  private <T> T defaultValue(Class<? extends Annotation> annotationClass)
      throws IllegalAccessException, InvocationTargetException, NoSuchMethodException, SecurityException
  {
    Method method = annotationClass.getMethod("value");
    Annotation annotation = annotationClass.getAnnotation(annotationClass);
    Preconditions.checkNotNull(
        annotation,
        StringUtils.format("Annotation class [%s] must be annotated with itself to set default value", annotationClass)
    );
    return (T) method.invoke(annotation);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(minTopNThreshold, numMergeBuffers, resultCache, componentSupplier);
  }

  @Override
  public boolean equals(Object obj)
  {
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    SqlTestFrameworkConfig other = (SqlTestFrameworkConfig) obj;
    return minTopNThreshold == other.minTopNThreshold
        && numMergeBuffers == other.numMergeBuffers
        && resultCache == other.resultCache
        && componentSupplier == other.componentSupplier;
  }

  public static class SqlTestFrameworkConfigStore implements Closeable
  {
    Map<SqlTestFrameworkConfig, ConfigurationInstance> configMap = new HashMap<>();

    public ConfigurationInstance getConfigurationInstance(
        SqlTestFrameworkConfig config,
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper) throws Exception
    {
      ConfigurationInstance ret = configMap.get(config);
      if (!configMap.containsKey(config)) {
        ret = new ConfigurationInstance(config, queryComponentSupplierWrapper);
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

  public static List<Annotation> collectAnnotations(Class<?> testClass, Method method)
  {
    List<Annotation> annotations = new ArrayList<>(Arrays.asList(method.getAnnotations()));
    Class<?> clz = testClass;
    while (clz != null) {
      annotations.addAll(Arrays.asList(clz.getAnnotations()));
      clz = clz.getSuperclass();
    }
    annotations.removeIf(
        annotation -> annotation.annotationType().getDeclaringClass() != SqlTestFrameworkConfig.class
    );
    return annotations;
  }

  /**
   * @see {@link SqlTestFrameworkConfig}
   */
  public static class Rule implements AfterAllCallback, BeforeEachCallback
  {
    SqlTestFrameworkConfigStore configStore = new SqlTestFrameworkConfigStore();
    private SqlTestFrameworkConfig config;
    private Method method;

    @Override
    public void afterAll(ExtensionContext context)
    {
      configStore.close();
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
      setConfig(context);
    }

    private void setConfig(ExtensionContext context)
    {
      method = context.getTestMethod().get();
      Class<?> testClass = context.getTestClass().get();
      List<Annotation> annotations = collectAnnotations(testClass, method);
      config = new SqlTestFrameworkConfig(annotations);
    }

    public SqlTestFrameworkConfig getConfig()
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

  public static class ConfigurationInstance
  {
    public SqlTestFramework framework;

    ConfigurationInstance(SqlTestFrameworkConfig config, QueryComponentSupplier testHost)
    {

      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .catalogResolver(testHost.createCatalogResolver())
          .minTopNThreshold(config.minTopNThreshold)
          .mergeBufferCount(config.numMergeBuffers)
          .withOverrideModule(config.resultCache.makeModule());
      framework = builder.build();
    }

    public ConfigurationInstance(
        SqlTestFrameworkConfig config,
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper) throws Exception
    {
      this(config, queryComponentSupplierWrapper.apply(makeQueryComponentSupplier(config.componentSupplier)));
    }

    private static QueryComponentSupplier makeQueryComponentSupplier(
        Class<? extends QueryComponentSupplier> supplierClazz) throws Exception
    {
      Constructor<? extends QueryComponentSupplier> constructor = supplierClazz.getConstructor(TempDirProducer.class);
      return constructor.newInstance(new TempDirProducer("druid-test"));
    }

    public void close()
    {
      framework.close();
    }
  }
}
