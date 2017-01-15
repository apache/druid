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

package io.druid.common.guava;

import com.google.common.base.Supplier;

/**
 * A settable Supplier. Not thread safe.
 */
public class SettableSupplier<T> implements Supplier<T>
{
  private T obj;

  public SettableSupplier()
  {
  }

  public SettableSupplier(T initialValue)
  {
    obj = initialValue;
  }

  public void set(T obj)
  {
    this.obj = obj;
  }

  @Override
  public T get()
  {
    return obj;
  }
}
