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

package org.apache.druid.query.rowsandcols.semantic;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.SemanticCreator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Ensures that the usage of the {@link SemanticCreator} annotations follows some basic rules.
 */
@RunWith(Parameterized.class)
public class SemanticCreatorUsageTest
{

  private final Method method;

  @Parameters(name = "{0}")
  public static List<Object[]> getParameters()
  {
    List<Object[]> params = new ArrayList<Object[]>();
    Set<Method> methodsAnnotatedWith = new Reflections("org.apache", new MethodAnnotationsScanner())
        .getMethodsAnnotatedWith(SemanticCreator.class);

    for (Method method : methodsAnnotatedWith) {
      String simpleMethodName = method.getDeclaringClass().getSimpleName() + "#" + method.getName();
      params.add(new Object[] {simpleMethodName, method});
    }
    params.sort(Comparator.comparing(o -> (String) o[0]));

    return params;
  }

  public SemanticCreatorUsageTest(@SuppressWarnings("unused") String simpleMethodName, Method method)
  {
    this.method = method;
  }

  /**
   * {@link SemanticCreator} methods must be public to be accessible by the creator.
   */
  @Test
  public void testPublic()
  {
    int modifiers = method.getModifiers();
    assertTrue(StringUtils.format("method [%s] is not public", method), Modifier.isPublic(modifiers));
  }

  /**
   * {@link SemanticCreator} must return with an interface.
   *
   * An exact implementation may indicate that some interface methods might be missing.
   */
  @Test
  public void testReturnType()
  {
    Class<?> returnType = method.getReturnType();
    assertTrue(
        returnType + " is not an interface; this method must return with an interface; ",
        returnType.isInterface()
    );
  }

  /**
   * {@link SemanticCreator} method names must follow the naming pattern toReturnType().
   *
   * For example: a method returning with a type of Ball should be named as "toBall"
   */
  @Test
  public void testMethodName()
  {
    Class<?> returnType = method.getReturnType();

    String desiredMethodName = "to" + returnType.getSimpleName();
    assertEquals("should be named as " + desiredMethodName, desiredMethodName, method.getName());

  }
}
