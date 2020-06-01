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

package org.apache.druid.annotations;

import org.junit.Assert;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class SubclassesMustOverrideEqualsAndHashCodeTest
{
  @Test
  public void testEqualsAndHashCode() throws NoSuchMethodException
  {
    // Exclude test classes
    Set<URL> urls = ClasspathHelper.forPackage("org.apache.druid")
                                   .stream()
                                   .filter(url -> !url.toString().contains("/target/test-classes"))
                                   .collect(Collectors.toSet());
    Reflections reflections = new Reflections(urls);
    Set<Class<?>> classes = reflections.getTypesAnnotatedWith(SubclassesMustOverrideEqualsAndHashCode.class);
    Set<String> failed = new HashSet<>();
    for (Class<?> clazz : classes) {
      if (clazz.isInterface() || Modifier.isAbstract(clazz.getModifiers())) {
        continue;
      }
      Method m = clazz.getMethod("hashCode");
      String className = clazz.getName();
      try {
        Assert.assertNotSame(className + " does not implment hashCode", Object.class, m.getDeclaringClass());
      }
      catch (AssertionError e) {
        failed.add(className);
      }
    }
    if (!failed.isEmpty()) {
      System.err.println("failed classes [" + failed.size() + "] : ");
      failed.forEach(c -> System.err.println("\t" + c));
      Assert.fail();
    }
  }
}
