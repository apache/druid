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

package org.apache.druid.sql.calcite.aggregation;

import com.google.inject.BindingAnnotation;
import org.apache.druid.error.DruidException;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
@BindingAnnotation
public @interface NativelySupportsDistinct
{
  boolean value() default false;

  class NativelySupportsDistinctProcessor implements InvocationHandler
  {
    private final Object target;

    public NativelySupportsDistinctProcessor(Object target) {
      this.target = target;
    }

    @Override
    public Object invoke(Object object, Method method, Object[] args) throws Throwable {
      Class<?> clazz = target.getClass();

      if (object instanceof SqlAggregator) {
        SqlAggregator aggregator = (SqlAggregator) object;
        //if (PlannerConfig.)
        if (!clazz.isAnnotationPresent(NativelySupportsDistinct.class)
            || !clazz.getAnnotation(NativelySupportsDistinct.class).value()) {
          throw DruidException.forPersona(DruidException.Persona.USER).ofCategory(DruidException.Category.UNSUPPORTED)
                              .build("Aggregation [%s] is not supported with useApproximateCountDistinct enabled.", aggregator.calciteFunction().getKind());
        }
      }
      // Invoke the actual method
      return method.invoke(target, args);
    }
  }
}
