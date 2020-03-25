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

package org.apache.druid.query.aggregation;

import org.apache.druid.java.util.common.Pair;
import org.apache.druid.query.monomorphicprocessing.HotLoopCallee;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

class RecordingRuntimeShapeInspector implements RuntimeShapeInspector
{
  private final List<Pair<String, Object>> visited = new ArrayList<>();

  @Override
  public void visit(String fieldName, @Nullable HotLoopCallee value)
  {
    visited.add(Pair.of(fieldName, null));
    if (value != null) {
      value.inspectRuntimeShape(this);
    }
  }

  @Override
  public void visit(String fieldName, @Nullable Object value)
  {
    visited.add(Pair.of(fieldName, value));
  }

  @Override
  public <T> void visit(String fieldName, T[] values)
  {
    visited.add(Pair.of(fieldName, values));
  }

  @Override
  public void visit(String flagName, boolean flagValue)
  {
    visited.add(Pair.of(flagName, flagName));
  }

  @Override
  public void visit(String key, String runtimeShape)
  {
    visited.add(Pair.of(key, runtimeShape));
  }

  public List<Pair<String, Object>> getVisited()
  {
    return visited;
  }
}
