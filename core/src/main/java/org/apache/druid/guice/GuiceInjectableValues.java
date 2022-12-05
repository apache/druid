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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.InjectableValues;
import com.google.inject.ConfigurationException;
import com.google.inject.Injector;
import com.google.inject.Key;
import org.apache.druid.java.util.common.IAE;

import javax.annotation.Nullable;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicReference;

/**
 *
 */
public class GuiceInjectableValues extends InjectableValues
{
  private final Injector injector;
  private final AtomicReference<HashSet<Key>> nullables;

  public GuiceInjectableValues(Injector injector)
  {
    this.injector = injector;
    this.nullables = new AtomicReference<>(new HashSet<>());
  }

  @Override
  public Object findInjectableValue(
      Object valueId,
      DeserializationContext ctxt,
      BeanProperty forProperty,
      Object beanInstance
  )
  {
    // From the docs:   "Object that identifies value to inject; may be a simple name or more complex identifier object,
    //                  whatever provider needs"
    // Currently we should only be dealing with `Key` instances, and anything more advanced should be handled with
    // great care
    if (nullables.get().contains((Key) valueId)) {
      return null;
    } else if (valueId instanceof Key) {
      try {
        return injector.getInstance((Key) valueId);
      }
      catch (ConfigurationException ce) {
        // check if nullable annotation is present for this
        if (forProperty.getAnnotation(Nullable.class) != null) {
          HashSet<Key> encounteredNullables = new HashSet<>(nullables.get());
          encounteredNullables.add((Key) valueId);
          nullables.set(encounteredNullables);
          return null;
        }
        throw ce;
      }
    }
    throw new IAE(
        "Unknown class type [%s] for valueId [%s]",
        valueId.getClass().getName(),
        valueId.toString()
    );
  }
}
