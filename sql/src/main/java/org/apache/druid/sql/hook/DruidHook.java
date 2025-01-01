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

import com.google.errorprone.annotations.Immutable;
import org.apache.calcite.rel.RelNode;
import org.apache.druid.query.Query;
import java.util.Objects;

/**
 * Interface for hooks that can be invoked by {@link DruidHookDispatcher}.
 *
 * HookKey should be added at every place a new hook is needed.
 */
@FunctionalInterface
public interface DruidHook<T>
{
  HookKey<RelNode> CONVERTED_PLAN = new HookKey<>("converted", RelNode.class);
  HookKey<RelNode> LOGICAL_PLAN = new HookKey<>("logicalPlan", RelNode.class);
  HookKey<RelNode> DRUID_PLAN = new HookKey<>("druidPlan", RelNode.class);
  HookKey<String> SQL = new HookKey<>("sql", String.class);
  HookKey<String> MSQ_PLAN = new HookKey<>("msqPlan", String.class);
  @SuppressWarnings("rawtypes")
  HookKey<Query> NATIVE_PLAN = new HookKey<>("nativePlan", Query.class);

  @Immutable
  class HookKey<T>
  {
    private final String label;
    private final Class<T> type;

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

  void invoke(HookKey<T> key, T object);
}
