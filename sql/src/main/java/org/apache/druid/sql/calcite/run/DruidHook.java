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

package org.apache.druid.sql.calcite.run;

import org.apache.calcite.rel.RelNode;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@FunctionalInterface

public interface DruidHook<T>
{
  static class HookKey<T>
  {
    private String label;
    private Class<T> type;

    public HookKey(String label, Class<T> type)
    {
      this.label = label;
      this.type = type;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(label, type);
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      HookKey<?> other = (HookKey<?>) obj;
      return Objects.equals(label, other.label) && Objects.equals(type, other.type);
    }

  }

  public static final HookKey<RelNode> CONVERTED_PLAN = new HookKey<>("converted", RelNode.class);
  public static final HookKey<RelNode> LOGICAL_PLAN = new HookKey<>("logicalPlan", RelNode.class);
  public static final HookKey<RelNode> DRUID_PLAN = new HookKey<>("druidPlan", RelNode.class);
  public static final HookKey<String> SQL = new HookKey<>("sql", String.class);

  void invoke(HookKey<T> key, T object);

  static Map<HookKey<?>, List<DruidHook>> GLOBAL = new HashMap<>();

  public static void register(HookKey<?> label, DruidHook hook)
  {
    GLOBAL.computeIfAbsent(label, k -> new ArrayList<>()).add(hook);
  }

  public static void unregister(HookKey<?> key, DruidHook hook)
  {
    GLOBAL.get(key).remove(hook);
  }

  public static <T> Closeable  withHook(HookKey<T> key, DruidHook<T> hook)
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

  public static <T> void dispatch(HookKey<T> key, T object)
  {
    List<DruidHook> hooks = GLOBAL.get(key);
    if (hooks != null) {
      for (DruidHook hook : hooks) {
        hook.invoke(key, object);
      }
    }
  }
}
