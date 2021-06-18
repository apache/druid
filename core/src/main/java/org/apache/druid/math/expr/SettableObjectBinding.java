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

package org.apache.druid.math.expr;

import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

/**
 * Simple map backed object binding
 */
public class SettableObjectBinding implements Expr.ObjectBinding
{
  private final Map<String, Object> bindings;

  public SettableObjectBinding()
  {
    this.bindings = new HashMap<>();
  }

  public SettableObjectBinding(int expectedSize)
  {
    this.bindings = Maps.newHashMapWithExpectedSize(expectedSize);
  }

  @Nullable
  @Override
  public Object get(String name)
  {
    return bindings.get(name);
  }

  public SettableObjectBinding withBinding(String name, @Nullable Object value)
  {
    bindings.put(name, value);
    return this;
  }
}
