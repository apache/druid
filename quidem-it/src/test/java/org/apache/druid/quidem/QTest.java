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

package org.apache.druid.quidem;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.druid.sql.calcite.util.SqlTestFramework.QueryComponentSupplier;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

public class QTest extends DruidQuidemTestBase
{
  public QTest()
  {
    super();
  }

  @Override
  protected File getTestRoot()
  {
    return ProjectPathUtils.getPathFromProjectRoot("quidem-it/src/test/quidem/" + getClass().getName());
  }

  @Test
  public void ensureNoRecordFilesPresent() throws IOException
  {
    // ensure that the captured ones are saved into this test's input path
    assertEquals(QuidemCaptureResource.RECORD_PATH, getTestRoot());
    for (String name : getFileNames()) {
      if (name.startsWith("record-")) {
        fail("Record file found: " + name);
      }
    }
  }

  @Test
  public void testQTest() throws Exception
  {
    LoadingCache<String, Set<Class<? extends QueryComponentSupplier>>> componentSupplierClassCache = CacheBuilder
        .newBuilder()
        .build(new CacheLoader<String, Set<Class<? extends QueryComponentSupplier>>>()
        {
          @Override
          public Set<Class<? extends QueryComponentSupplier>> load(String pkg) throws MalformedURLException
          {
            return new Reflections(
                new ConfigurationBuilder()
                    // .addUrls(ClasspathHelper.forPackage(pkg,
                    // getClass().getClassLoader()))

                    // .addClassLoaders(getClass().getClassLoader().getParent())
                    .setScanners(
                        new SubTypesScanner(true)

                    )
                    .filterInputsBy(
                        new FilterBuilder().includePackage(pkg).and(
                            s -> s.contains("ComponentSupplier")
                        )

                    )
                    .setUrls(org.reflections.util.ClasspathHelper.forJavaClassPath())

            )
                .getSubTypesOf(QueryComponentSupplier.class);
          }
        });

    Set<Class<? extends QueryComponentSupplier>> a = componentSupplierClassCache.get("");
    throw new RuntimeException("X" + a);
  }
}
