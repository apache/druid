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

import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provider;

import java.util.ArrayList;
import java.util.List;

/**
 */
public class ListProvider<T> implements Provider<List<T>>
{
  private final List<Key<? extends T>> itemsToLoad = new ArrayList<>();
  private Injector injector;

  public ListProvider<T> add(Class<? extends T> clazz)
  {
    return add(Key.get(clazz));
  }

  public ListProvider<T> add(Key<? extends T> key)
  {
    itemsToLoad.add(key);
    return this;
  }

  @Inject
  private void configure(Injector injector)
  {
    this.injector = injector;
  }

  @Override
  public List<T> get()
  {
    List<T> retVal = Lists.newArrayListWithExpectedSize(itemsToLoad.size());
    for (Key<? extends T> key : itemsToLoad) {
      retVal.add(injector.getInstance(key));
    }
    return retVal;
  }
}
