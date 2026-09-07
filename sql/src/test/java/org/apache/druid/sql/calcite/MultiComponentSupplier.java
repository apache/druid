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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Module;
import org.apache.druid.error.DruidException;
import org.apache.druid.guice.DruidInjectorBuilder;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.java.util.common.io.Closer;
import org.apache.druid.query.QueryRunnerFactoryConglomerate;
import org.apache.druid.query.lookup.LookupExtractorFactoryContainerProvider;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.server.SpecificSegmentsQuerySegmentWalker;
import org.apache.druid.sql.calcite.run.SqlEngine;
import org.apache.druid.sql.calcite.util.SqlTestFramework;
import org.apache.druid.sql.calcite.util.SqlTestFramework.PlannerComponentSupplier;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Multi supplier to run the same testcase against multiple suppliers.
 *
 * Usage:
 *
 * <pre>
 * @ComponentSuppliers( {  RealSupplier1.class, RealSupplier2.class } )
 * public class SomeSupplier extends MultiComponentSupplier {
 * }
 * </pre>
 */
public abstract class MultiComponentSupplier implements QueryComponentSupplier
{
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  public @interface ComponentSuppliers
  {
    Class<? extends QueryComponentSupplier>[] value();
  }

  private static final Map<Class<? extends QueryComponentSupplier>, Set<Class<? extends MultiComponentSupplier>>>
      KNOWN_SUB_QUERY_COMPONENT_SUPPLIERS = new LinkedHashMap<>();

  public static void registerComponentSupplier(Class<? extends MultiComponentSupplier> clazz)
  {
    final List<Class<? extends QueryComponentSupplier>> subSuppliers = getSuppliers(clazz);
    for (Class<? extends QueryComponentSupplier> subSupplier : subSuppliers) {
      KNOWN_SUB_QUERY_COMPONENT_SUPPLIERS.computeIfAbsent(subSupplier, ignore -> new LinkedHashSet<>()).add(clazz);
    }
  }

  public static Set<Class<? extends MultiComponentSupplier>> findParentSuppliers(
      Class<? extends QueryComponentSupplier> clazz
  )
  {
    return KNOWN_SUB_QUERY_COMPONENT_SUPPLIERS.get(clazz);
  }

  public static List<Class<? extends QueryComponentSupplier>> getSuppliers(Class<? extends MultiComponentSupplier> clazz)
  {
    ComponentSuppliers a = clazz.getAnnotation(ComponentSuppliers.class);
    if (a == null || a.value().length == 0) {
      throw DruidException.defensive("No component suppliers found [%s].", clazz.getName());
    }
    return Arrays.asList(a.value());
  }

  private DruidException unsupportedException()
  {
    return DruidException.defensive("Unexpected call made to " + getClass().getName());
  }

  @Override
  public void gatherProperties(Properties properties)
  {
    throw unsupportedException();
  }

  @Override
  public DruidModule getCoreModule()
  {
    throw unsupportedException();
  }

  @Override
  public DruidModule getOverrideModule()
  {
    throw unsupportedException();
  }

  @Override
  public SpecificSegmentsQuerySegmentWalker addSegmentsToWalker(
      SpecificSegmentsQuerySegmentWalker walker,
      ObjectMapper jsonMapper
  )
  {
    throw unsupportedException();
  }

  @Override
  public Class<? extends SqlEngine> getSqlEngineClass()
  {
    throw unsupportedException();
  }

  @Override
  public JoinableFactoryWrapper createJoinableFactoryWrapper(LookupExtractorFactoryContainerProvider lookupProvider)
  {
    throw unsupportedException();
  }

  @Override
  public void finalizeTestFramework(SqlTestFramework sqlTestFramework)
  {
    throw unsupportedException();
  }

  @Override
  public PlannerComponentSupplier getPlannerComponentSupplier()
  {
    throw unsupportedException();
  }

  @Override
  public void configureGuice(DruidInjectorBuilder injectorBuilder, List<Module> overrideModules)
  {
    throw unsupportedException();
  }

  @Override
  public Boolean isExplainSupported()

  {
    throw unsupportedException();
  }

  @Override
  public QueryRunnerFactoryConglomerate wrapConglomerate(QueryRunnerFactoryConglomerate conglomerate,
      Closer resourceCloser)
  {
    throw unsupportedException();
  }

  @Override
  public TempDirProducer getTempDirProducer()
  {
    throw unsupportedException();
  }
}
