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

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utilities for building a Guice injector. Defined as a parameterized
 * type so that this class can be used in derived fluent builders.
 */
public class BaseInjectorBuilder<T extends BaseInjectorBuilder<?>>
{
  private final List<Module> modules = new ArrayList<>();

  @SuppressWarnings("unchecked")
  public T add(Module... modules)
  {
    // Done this way because IntelliJ inspections complains if we
    // try to iterate, because it thinks addAll() accepts an array,
    // which it does not.
    this.modules.addAll(Arrays.asList(modules));
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T addAll(List<Module> modules)
  {
    this.modules.addAll(modules);
    return (T) this;
  }

  @SuppressWarnings("unchecked")
  public T addAll(Iterable<? extends Module> modules)
  {
    for (Module m : modules) {
      this.modules.add(m);
    }
    return (T) this;
  }

  public Injector build()
  {
    return Guice.createInjector(modules);
  }
}
