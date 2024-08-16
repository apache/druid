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

import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.sql.calcite.DrillWindowQueryTest.DrillComponentSupplier;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.MinTopNThreshold;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.NumMergeBuffers;
import org.apache.druid.sql.calcite.SqlTestFrameworkConfig.ResultCache;
import org.apache.druid.sql.calcite.util.CacheTestHelperModule.ResultCacheMode;
import org.junit.jupiter.api.Test;

import java.lang.annotation.Annotation;
import java.net.URI;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SqlTestFrameworkConfigTest
{
  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(SqlTestFrameworkConfig.class)
        .usingGetClass()
        .verify();
  }

  @ResultCache(ResultCacheMode.ENABLED)
  static class B
  {
  }

  @NumMergeBuffers(3)
  static class C extends B
  {
    @MinTopNThreshold(1)
    public void imaginaryTestMethod1()
    {
    }

    public void imaginaryTestMethod2()
    {
    }
  }

  @MinTopNThreshold(2)
  static class D extends C
  {
    @NumMergeBuffers(1)
    public void imaginaryTestMethod3()
    {
    }

    @ResultCache(ResultCacheMode.DISABLED)
    @NumMergeBuffers(1)
    @MinTopNThreshold(1)
    public void imaginaryTestMethod4()
    {
    }
  }

  @Test
  public void testAnnotationProcessingChain() throws Exception
  {
    List<Annotation> annotations = SqlTestFrameworkConfig
        .collectAnnotations(C.class, D.class.getMethod("imaginaryTestMethod1"));
    SqlTestFrameworkConfig config = new SqlTestFrameworkConfig(annotations);
    assertEquals(1, config.minTopNThreshold);
    assertEquals(3, config.numMergeBuffers);
    assertEquals(ResultCacheMode.ENABLED, config.resultCache);
  }

  @Test
  public void testAnnotationOverridingClassHasDefault() throws Exception
  {
    List<Annotation> annotations = SqlTestFrameworkConfig
        .collectAnnotations(D.class, D.class.getMethod("imaginaryTestMethod2"));
    SqlTestFrameworkConfig config = new SqlTestFrameworkConfig(annotations);
    assertEquals(2, config.minTopNThreshold);
    assertEquals(3, config.numMergeBuffers);
    assertEquals(ResultCacheMode.ENABLED, config.resultCache);
  }

  @Test
  public void testAnnotationOverridingClassChangesDefault() throws Exception
  {
    List<Annotation> annotations = SqlTestFrameworkConfig
        .collectAnnotations(D.class, D.class.getMethod("imaginaryTestMethod3"));
    SqlTestFrameworkConfig config = new SqlTestFrameworkConfig(annotations);
    assertEquals(2, config.minTopNThreshold);
    assertEquals(1, config.numMergeBuffers);
    assertEquals(ResultCacheMode.ENABLED, config.resultCache);
  }

  @Test
  public void testAnnotationsAtTestLevel() throws Exception
  {
    List<Annotation> annotations = SqlTestFrameworkConfig
        .collectAnnotations(D.class, D.class.getMethod("imaginaryTestMethod4"));
    SqlTestFrameworkConfig config = new SqlTestFrameworkConfig(annotations);
    assertEquals(1, config.minTopNThreshold);
    assertEquals(1, config.numMergeBuffers);
    assertEquals(ResultCacheMode.DISABLED, config.resultCache);
  }

  @Test
  public void testInvalidConfigKeySpecified()
  {
    ImmutableMap<String, String> configMap = ImmutableMap.<String, String>builder()
        .put("nonExistent", "someValue")
        .build();
    IAE e = assertThrows(
        IAE.class,
        () -> new SqlTestFrameworkConfig(configMap)
    );

    assertEquals(
        "Invalid configuration key(s) specified [[nonExistent]]; valid options are [[numMergeBuffers, minTopNThreshold, resultCache, componentSupplier]]",
        e.getMessage()
    );
  }

  @Test
  public void testURI()
  {
    SqlTestFrameworkConfig c = new SqlTestFrameworkConfig(
        ImmutableMap.of(
            "componentSupplier", DrillComponentSupplier.class.getSimpleName()
        )
    );
    URI uri = c.getDruidTestURI();
    assertEquals("druidtest:///?componentSupplier=DrillComponentSupplier", uri.toString());
  }
}
