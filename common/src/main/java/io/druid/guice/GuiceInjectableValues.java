/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.guice;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.google.inject.Injector;
import com.google.inject.Key;

import io.druid.java.util.common.IAE;

/**
 */
public class GuiceInjectableValues extends InjectableValues
{
  private final Injector injector;

  public GuiceInjectableValues(Injector injector) {this.injector = injector;}

  @Override
  public Object findInjectableValue(
      Object valueId, DeserializationContext ctxt, BeanProperty forProperty, Object beanInstance
  )
  {
    // From the docs:   "Object that identifies value to inject; may be a simple name or more complex identifier object,
    //                  whatever provider needs"
    // Currently we should only be dealing with `Key` instances, and anything more advanced should be handled with
    // great care
    if (valueId instanceof Key) {
      return injector.getInstance((Key) valueId);
    }
    throw new IAE(
        "Unknown class type [%s] for valueId [%s]",
        valueId.getClass().getCanonicalName(),
        valueId.toString()
    );
  }
}
