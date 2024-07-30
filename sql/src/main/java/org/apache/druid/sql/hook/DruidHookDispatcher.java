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

package org.apache.druid.sql.hook;

import com.google.inject.Inject;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.sql.hook.DruidHook.HookKey;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@LazySingleton
public class DruidHookDispatcher
{
  @Inject
  public DruidHookDispatcher()
  {
  }

  Map<HookKey<?>, List<DruidHook<?>>> GLOBAL = new HashMap<>();

  public void register(HookKey<?> label, DruidHook<?> hook)
  {
    GLOBAL.computeIfAbsent(label, k -> new ArrayList<>()).add(hook);
  }

  public void unregister(HookKey<?> key, DruidHook<?> hook)
  {
    GLOBAL.get(key).remove(hook);
  }

  public <T> Closeable withHook(HookKey<T> key, DruidHook<T> hook)
  {
    register(key, hook);
    return new Closeable()
    {
      @Override
      public void close()
      {
        unregister(key, hook);
      }
    };
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public <T> void dispatch(HookKey<T> key, T object)
  {
    List<DruidHook<?>> hooks = GLOBAL.get(key);
    if (hooks != null) {
      for (DruidHook hook : hooks) {
        hook.invoke(key, object);
      }
    }
  }

}
