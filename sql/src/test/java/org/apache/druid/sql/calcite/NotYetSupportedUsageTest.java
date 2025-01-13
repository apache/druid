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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.druid.sql.calcite.DecoupledTestConfig.QuidemTestCaseReason;
import org.apache.druid.sql.calcite.NotYetSupported.Modes;
import org.junit.Test;
import org.reflections.Reflections;
import org.reflections.scanners.MethodAnnotationsScanner;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class NotYetSupportedUsageTest
{
  @Test
  public void ensureAllModesUsed()
  {
    Set<Method> methodsAnnotatedWith = getAnnotatedMethods();

    Set<NotYetSupported.Modes> modes = new HashSet<>(Arrays.asList(NotYetSupported.Modes.values()));
    for (Method method : methodsAnnotatedWith) {
      NotYetSupported annot = method.getAnnotation(NotYetSupported.class);
      modes.remove(annot.value());
    }

    assertEquals("There are unused modes which should be removed", Collections.emptySet(), modes);
  }

  static class ReportEntry
  {
    public static final Comparator<ReportEntry> CLASS_NCASES_MODE_COMPARATOR = new Comparator<>()
    {
      @Override
      public int compare(ReportEntry l, ReportEntry r)
      {
        int res = l.className.compareTo(r.className);
        if (res != 0) {
          return res;
        }
        res = -Integer.compare(l.methodNames.size(), r.methodNames.size());
        if (res != 0) {
          return res;
        }
        return 0;
      }
    };

    public String className;
    private List<String> methodNames;
    private Enum<?> mode;

    public ReportEntry(String className, String methodName, Enum<?> mode)
    {
      this.className = className;
      this.mode = mode;
      methodNames = new ArrayList<>();
      methodNames.add(methodName);
    }

    public List<Object> getKey()
    {
      return ImmutableList.of(className, mode);
    }

    public void merge(ReportEntry entry)
    {
      methodNames.addAll(entry.methodNames);
    }

    @Override
    public String toString()
    {
      return " | " + className + " | " + methodNames.size() + " | " + mode.name() + " | " + mode + " | ";
    }
  }

  private Set<Method> getAnnotatedMethods()
  {
    return new Reflections("org.apache.druid.sql", new MethodAnnotationsScanner())
        .getMethodsAnnotatedWith(NotYetSupported.class);
  }

  @Test
  public void createModesReport()
  {
    Set<Method> methodsAnnotatedWith = getAnnotatedMethods();
    Map<Method, Modes> map = Maps.asMap(methodsAnnotatedWith, this::getModesAnnotation);
    createReport(map);
  }

  private Set<Method> getDecoupledTestConfigAnnotatedMethods()
  {
    return new Reflections("org.apache.druid.sql", new MethodAnnotationsScanner())
        .getMethodsAnnotatedWith(DecoupledTestConfig.class);
  }

  @Test
  public void createQuidemReasonReport()
  {
    Set<Method> methodsAnnotatedWith = getDecoupledTestConfigAnnotatedMethods();
    Map<Method, QuidemTestCaseReason> map = Maps.asMap(methodsAnnotatedWith, this::getQuidemReasonAnnotations);
    createReport(map);
  }

  private Modes getModesAnnotation(Method method)
  {
    NotYetSupported annotation = method.getAnnotation(NotYetSupported.class);
    if (annotation == null) {
      return null;
    }
    return annotation.value();
  }

  private QuidemTestCaseReason getQuidemReasonAnnotations(Method method)
  {
    DecoupledTestConfig annotation = method.getAnnotation(DecoupledTestConfig.class);
    if (annotation == null) {
      return null;
    }
    return annotation.quidemReason();
  }

  private <E extends Enum<?>> void createReport(Map<Method, E> methodsAnnotatedWith)
  {
    Map<List<Object>, ReportEntry> mentryMap = new HashMap<>();
    for (Entry<Method, E> e : methodsAnnotatedWith.entrySet()) {
      Method method = e.getKey();
      ReportEntry entry = new ReportEntry(
          method.getDeclaringClass().getSimpleName(),
          method.getName(),
          e.getValue()
      );
      ReportEntry existing = mentryMap.get(entry.getKey());
      if (existing != null) {
        existing.merge(entry);
      } else {
        mentryMap.put(entry.getKey(), entry);
      }
    }
    ArrayList<ReportEntry> results = new ArrayList<>(mentryMap.values());
    results.sort(ReportEntry.CLASS_NCASES_MODE_COMPARATOR);
    for (ReportEntry reportEntry : results) {
      System.out.println(reportEntry);
    }
  }

}
