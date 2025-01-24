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

import com.google.common.base.CaseFormat;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.topn.TopNQueryConfig;
import org.apache.druid.quidem.DruidAvaticaTestDriver;
import org.apache.druid.server.QueryStackTests;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.StandardComponentSupplier;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.client.utils.URLEncodedUtils;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.reflections.Configuration;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import javax.annotation.Nonnull;
import java.io.Closeable;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
  @NumMergeBuffers(QueryStackTests.DEFAULT_NUM_MERGE_BUFFERS)
  public @interface NumMergeBuffers
  {
    ConfigOptionProcessor<Integer> PROCESSOR = new ConfigOptionProcessor<>(NumMergeBuffers.class)
    {
      @Override
      public Integer fromString(String str) throws NumberFormatException
      {
        return Integer.valueOf(str);
      }
    };

    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @MinTopNThreshold(TopNQueryConfig.DEFAULT_MIN_TOPN_THRESHOLD)
  public @interface MinTopNThreshold
  {
    ConfigOptionProcessor<Integer> PROCESSOR = new ConfigOptionProcessor<>(MinTopNThreshold.class)
    {
      @Override
      public Integer fromString(String str) throws NumberFormatException
      {
        return Integer.valueOf(str);
      }
    };

    int value();
  }

  @Retention(RetentionPolicy.RUNTIME)
  @Target({ElementType.METHOD, ElementType.TYPE})
  @ResultCache(ResultCacheMode.DISABLED)
  public @interface ResultCache
  {
    ConfigOptionProcessor<ResultCacheMode> PROCESSOR = new ConfigOptionProcessor<>(ResultCache.class)
    {
      @Override
      public ResultCacheMode fromString(String str)
      {
        return ResultCacheMode.valueOf(str);
      }
    };

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
    ConfigOptionProcessor<Class<? extends QueryComponentSupplier>> PROCESSOR = new ConfigOptionProcessor<>(
        ComponentSupplier.class
    )
    {
      @Override
      public Class<? extends QueryComponentSupplier> fromString(String name) throws Exception
      {
        return getQueryComponentSupplierForName(name);
      }
    };

    Class<? extends QueryComponentSupplier> value();
  }

  private static final Set<String> KNOWN_CONFIG_KEYS = ImmutableSet.<String>builder()
      .add(NumMergeBuffers.PROCESSOR.getConfigName())
      .add(MinTopNThreshold.PROCESSOR.getConfigName())
      .add(ResultCache.PROCESSOR.getConfigName())
      .add(ComponentSupplier.PROCESSOR.getConfigName())
      .build();

  public final int numMergeBuffers;
  public final int minTopNThreshold;
  public final ResultCacheMode resultCache;
  public final Class<? extends QueryComponentSupplier> componentSupplier;

  public SqlTestFrameworkConfig(List<Annotation> annotations)
  {
    try {
      numMergeBuffers = NumMergeBuffers.PROCESSOR.fromAnnotations(annotations);
      minTopNThreshold = MinTopNThreshold.PROCESSOR.fromAnnotations(annotations);
      resultCache = ResultCache.PROCESSOR.fromAnnotations(annotations);
      componentSupplier = ComponentSupplier.PROCESSOR.fromAnnotations(annotations);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public SqlTestFrameworkConfig(Map<String, String> queryParams)
  {
    validateConfigKeys(queryParams.keySet());
    try {
      numMergeBuffers = NumMergeBuffers.PROCESSOR.fromMap(queryParams);
      minTopNThreshold = MinTopNThreshold.PROCESSOR.fromMap(queryParams);
      resultCache = ResultCache.PROCESSOR.fromMap(queryParams);
      componentSupplier = ComponentSupplier.PROCESSOR.fromMap(queryParams);
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private void validateConfigKeys(Set<String> keySet)
  {
    Set<String> diff = Sets.difference(keySet, KNOWN_CONFIG_KEYS);
    if (diff.isEmpty()) {
      return;
    }
    throw new IAE("Invalid configuration key(s) specified [%s]; valid options are [%s]", diff, KNOWN_CONFIG_KEYS);
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
    private final Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper;

    public SqlTestFrameworkConfigStore(
        Function<QueryComponentSupplier, QueryComponentSupplier> queryComponentSupplierWrapper)
    {
      this.queryComponentSupplierWrapper = queryComponentSupplierWrapper;
    }

    Map<SqlTestFrameworkConfig, ConfigurationInstance> configMap = new HashMap<>();

    public ConfigurationInstance getConfigurationInstance(
        SqlTestFrameworkConfig config) throws Exception
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
    SqlTestFrameworkConfigStore configStore = new SqlTestFrameworkConfigStore(Function.identity());
    private SqlTestFrameworkConfig config;
    private Method method;
    private String testName;

    @Override
    public void afterAll(ExtensionContext context)
    {
      configStore.close();
    }

    @Override
    public void beforeEach(ExtensionContext context)
    {
      makeConfigFromContext(context);
    }

    public void makeConfigFromContext(ExtensionContext context)
    {
      testName = buildTestCaseName(context);
      method = context.getTestMethod().get();
      Class<?> testClass = context.getTestClass().get();
      List<Annotation> annotations = collectAnnotations(testClass, method);
      setConfig(new SqlTestFrameworkConfig(annotations));
    }

    public void setConfig(SqlTestFrameworkConfig config)
    {
      this.config = config;
    }

    /**
     * Returns a string identifying the testcase.
     *
     *
     */
    public String buildTestCaseName(ExtensionContext context)
    {
      List<String> names = new ArrayList<>();
      Pattern pattern = Pattern.compile("\\([^)]*\\)");
      // this will add all name pieces - except the "last" which would be the
      // Class level name
      do {
        names.add(0, pattern.matcher(context.getDisplayName()).replaceAll(""));
        context = context.getParent().get();
      } while (context.getTestMethod().isPresent());
      return Joiner.on("@").join(names);
    }

    public SqlTestFrameworkConfig getConfig()
    {
      return config;
    }

    public SqlTestFramework get() throws Exception
    {
      return configStore.getConfigurationInstance(config).framework;
    }

    public <T extends Annotation> T getAnnotation(Class<T> annotationType)
    {
      return method.getAnnotation(annotationType);
    }

    public String testName()
    {
      return testName;
    }
  }

  public static class ConfigurationInstance
  {
    public SqlTestFramework framework;

    ConfigurationInstance(SqlTestFrameworkConfig config, QueryComponentSupplier testHost)
    {
      SqlTestFramework.Builder builder = new SqlTestFramework.Builder(testHost)
          .withConfig(config)
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

  public URI getDruidTestURI()
  {
    try {
      Map<String, String> params = getNonDefaultMap();
      URIBuilder ub = new URIBuilder("druidtest:///");
      for (Entry<String, String> entry : params.entrySet()) {
        ub.setParameter(entry.getKey(), entry.getValue());
      }
      ub.setPath("///");
      return ub.build();
    }
    catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
  }

  private Map<String, String> getNonDefaultMap()
  {
    Map<String, String> map = new HashMap<>();
    SqlTestFrameworkConfig def = new SqlTestFrameworkConfig(Collections.emptyList());
    if (def.numMergeBuffers != numMergeBuffers) {
      map.put("numMergeBuffers", String.valueOf(numMergeBuffers));
    }
    if (def.minTopNThreshold != minTopNThreshold) {
      map.put("minTopNThreshold", String.valueOf(minTopNThreshold));
    }
    if (def.componentSupplier != componentSupplier) {
      map.put("componentSupplier", componentSupplier.getSimpleName());
    }
    if (!equals(new SqlTestFrameworkConfig(map))) {
      throw new IAE("Can't reproduce config via map!");
    }
    return map;
  }

  public static SqlTestFrameworkConfig fromURL(String url) throws SQLException
  {

    Map<String, String> queryParams;
    queryParams = new HashMap<>();
    try {
      URI uri = new URI(url);
      if (!DruidAvaticaTestDriver.SCHEME.equals(uri.getScheme())) {
        throw new SQLException(
            StringUtils.format("URI [%s] is invalid ; only scheme [%s] is supported.", url, DruidAvaticaTestDriver.SCHEME)
        );
      }
      if (uri.getHost() != null || uri.getPort() != -1) {
        throw new SQLException(StringUtils.format("URI [%s] is invalid ; only query parameters are supported.", url));
      }
      List<NameValuePair> params = URLEncodedUtils.parse(uri, StandardCharsets.UTF_8);
      for (NameValuePair pair : params) {
        queryParams.put(pair.getName(), pair.getValue());
      }
      // possible caveat: duplicate entries overwrite earlier ones
    }
    catch (URISyntaxException e) {
      throw new SQLException("Can't decode URI", e);
    }

    return new SqlTestFrameworkConfig(queryParams);
  }

  abstract static class ConfigOptionProcessor<T>
  {
    final Class<? extends Annotation> annotationClass;

    public ConfigOptionProcessor(Class<? extends Annotation> annotationClass)
    {
      this.annotationClass = annotationClass;
    }

    public final String getConfigName()
    {
      return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_CAMEL, annotationClass.getSimpleName());
    }

    @SuppressWarnings("unchecked")
    public final T fromAnnotations(List<Annotation> annotations) throws Exception
    {
      Method method = annotationClass.getMethod("value");
      for (Annotation annotation : annotations) {
        if (annotationClass.isInstance(annotation)) {
          return (T) method.invoke(annotation);
        }
      }
      return defaultValue();
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    public final T defaultValue() throws Exception
    {
      Method method = annotationClass.getMethod("value");
      Annotation annotation = annotationClass.getAnnotation(annotationClass);
      Preconditions.checkNotNull(
          annotation,
          StringUtils
              .format("Annotation class [%s] must be annotated with itself to set default value", annotationClass)
      );
      return (T) method.invoke(annotation);
    }

    public final T fromMap(Map<String, String> map) throws Exception
    {
      String key = getConfigName();
      String value = map.get(key);
      if (value == null) {
        return defaultValue();
      }
      return fromString(value);
    }

    public abstract T fromString(String str) throws Exception;
  }

  static LoadingCache<String, Set<Class<? extends QueryComponentSupplier>>> componentSupplierClassCache = CacheBuilder
      .newBuilder()
      .build(new CacheLoader<>()
      {
        @Override
        public Set<Class<? extends QueryComponentSupplier>> load(String pkg)
        {
          Configuration cfg = new ConfigurationBuilder()
              .setScanners(new SubTypesScanner(true))
              .setUrls(ClasspathHelper.forJavaClassPath())
              .filterInputsBy(
                  new FilterBuilder()
                      .includePackage(pkg)
                      .and(s -> s.contains("ComponentSupplier"))
              );
          return new Reflections(cfg).getSubTypesOf(QueryComponentSupplier.class);
        }
      });

  @Nonnull
  private static Class<? extends QueryComponentSupplier> getQueryComponentSupplierForName(String name) throws Exception
  {
    for (String pkg : new String[] {"org.apache.druid.sql.calcite", ""}) {
      Set<Class<? extends QueryComponentSupplier>> availableSuppliers = componentSupplierClassCache.get(pkg);
      for (Class<? extends QueryComponentSupplier> cl : availableSuppliers) {
        if (cl.getSimpleName().equals(name)) {
          return cl;
        }
      }
    }
    List<String> knownNames = componentSupplierClassCache.get("").stream().map(Class::getSimpleName)
        .collect(Collectors.toList());
    throw new IAE("ComponentSupplier [%s] is not known; known ones are [%s]", name, knownNames);
  }
}
