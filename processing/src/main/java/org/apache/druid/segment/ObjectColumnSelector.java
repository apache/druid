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

package org.apache.druid.segment;

/**
 * This class is convenient for implementation of "object-sourcing" {@link ColumnValueSelector}s, it provides default
 * implementations for all {@link ColumnValueSelector}'s methods except {@link #getObject()} and {@link
 * #classOfObject()}.
 *
 * This class should appear ONLY in "extends" clause or anonymous class creation, but NOT in "user" code, where
 * {@link BaseObjectColumnValueSelector} must be used instead.
 *
 * This is a class rather than an interface unlike {@link LongColumnSelector}, {@link FloatColumnSelector} and {@link
 * DoubleColumnSelector} solely in order to make {@link #isNull()} method final.
 */
public abstract class ObjectColumnSelector<T> implements ColumnValueSelector<T>
{
  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidental
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  public float getFloat()
  {
    T value = getObject();
    if (value == null) {
      return 0;
    }
    return ((Number) value).floatValue();
  }

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidental
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  public double getDouble()
  {
    T value = getObject();
    if (value == null) {
      return 0;
    }
    return ((Number) value).doubleValue();
  }

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector to minimize the probability of accidental
   * calling. "Polymorphism" of ObjectColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  public long getLong()
  {
    T value = getObject();
    if (value == null) {
      return 0;
    }
    return ((Number) value).longValue();
  }

  /**
   * @deprecated This method is marked as deprecated in ObjectColumnSelector since it always returns false.
   * There is no need to call this method.
   */
  @Deprecated
  @Override
  public final boolean isNull()
  {
    return false;
  }
}
