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

import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NotYetSupportedUsageTest
{
  @Test
  public void ensureAllModesUsed()
  {
    if (false) {
      ConfigurationBuilder configBuilder = new ConfigurationBuilder();
      configBuilder = configBuilder.setScanners(new MethodAnnotationsScanner());
//      if (true) {
//        configBuilder = configBuilder
//            .filterInputsBy(new FilterBuilder().include(FilterBuilder.prefix("org.apache.druid.sql")));
//      }
//      if (true) {
//        configBuilder = configBuilder.filterInputsBy(FilterBuilder.parse("+.*Test.*"));
//      }
      if (false) {
        configBuilder = configBuilder.setUrls(ClasspathHelper.forPackage("org.apache.druid"));
      } else {
        configBuilder = configBuilder.setUrls(ClasspathHelper.forClass(NotYetSupported.class));
      }

      Set<Method> methodsAnnotatedWith = new Reflections(configBuilder)
          .getMethodsAnnotatedWith(NotYetSupported.class);

      System.out.println(methodsAnnotatedWith);
      Set<NotYetSupported.Modes> modes = new HashSet<>(Arrays.asList(NotYetSupported.Modes.values()));
      for (Method method : methodsAnnotatedWith) {
        NotYetSupported annot = method.getAnnotation(NotYetSupported.class);
        modes.remove(annot.value());
      }

      assertEquals("There are unused modes which should be removed", Collections.emptySet(), modes);
    } else {

      Set<Method> methodsAnnotatedWith = new Reflections("org.apache", new MethodAnnotationsScanner())
          .getMethodsAnnotatedWith(NotYetSupported.class);

      Set<NotYetSupported.Modes> modes = new HashSet<>(Arrays.asList(NotYetSupported.Modes.values()));
      for (Method method : methodsAnnotatedWith) {
        NotYetSupported annot = method.getAnnotation(NotYetSupported.class);
        modes.remove(annot.value());
      }

      assertEquals("There are unused modes which should be removed", Collections.emptySet(), modes);
    }
  }
}
