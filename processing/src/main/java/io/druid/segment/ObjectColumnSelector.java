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

package io.druid.segment;

public interface ObjectColumnSelector<T> extends ColumnValueSelector
{
  public Class<T> classOfObject();

  /**
   * This method is not annotated with {@link io.druid.query.monomorphicprocessing.CalledFromHotLoop}, because
   * ObjectColumnSelector doesn't extend {@link io.druid.query.monomorphicprocessing.HotLoopCallee} yet. If it will,
   * this method should be annotated.
   */
  public T get();

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default float getFloat()
  {
    return ((Number) get()).floatValue();
  }

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default double getDouble()
  {
    return ((Number) get()).doubleValue();
  }

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default long getLong()
  {
    return ((Number) get()).longValue();
  }
}
