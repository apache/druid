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

import org.apache.druid.guice.annotations.ExtensionPoint;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;

import javax.annotation.Nullable;

/**
 * Object value selecting polymorphic "part" of the {@link ColumnValueSelector} interface. Users of {@link
 * ColumnValueSelector#getObject()} are encouraged to reduce the parameter/field/etc. type to
 * BaseObjectColumnValueSelector to make it impossible to accidently call any method other than {@link #getObject()}.
 *
 * All implementations of this interface MUST also implement {@link ColumnValueSelector}.
 *
 * Typically created by {@link ColumnSelectorFactory#makeColumnValueSelector(String)}.
 */
@ExtensionPoint
public interface BaseObjectColumnValueSelector<T>
{
  /**
   * Returns the currently-selected object.
   *
   * The behavior of this method depends on the type of selector, which can be determined by calling
   * {@link ColumnSelectorFactory#getColumnCapabilities(String)} on the same {@link ColumnSelectorFactory} that
   * you got this selector from. If the capabilties are nonnull, the selector type is given by
   * {@link ColumnCapabilities#getType()}.
   *
   * String selectors, where type is {@link ColumnType#STRING}, may return any type of object from this method,
   * especially in cases where the selector is casting objects to string at selection time. Callers are encouraged to
   * avoid the need to deal with various objects by using {@link ColumnSelectorFactory#makeDimensionSelector} instead.
   *
   * Numeric selectors, where {@link ColumnType#isNumeric()}, may return any type of {@link Number}. Callers that
   * wish to deal with more specific types should treat the original {@link ColumnValueSelector} as a
   * {@link BaseLongColumnValueSelector}, {@link BaseDoubleColumnValueSelector}, or
   * {@link BaseFloatColumnValueSelector} instead.
   *
   * Array selectors, where {@link ColumnType#isArray()}, must return {@code Object[]}. The array may contain
   * null elements, and the array itself may also be null.
   *
   * Selectors of unknown type, where {@link ColumnSelectorFactory#getColumnCapabilities(String)} returns null,
   * may return any type of object. Callers must be prepared for a wide variety of possible input objects. This case
   * is common during ingestion, where selectors are built on top of external data.
   */
  @Nullable
  T getObject();

  /**
   * Most-specific class of object returned by {@link #getObject()}, if known in advance. This method returns
   * {@link Object} when selectors do not know in advance what class of object they may return.
   */
  Class<? extends T> classOfObject();
}
