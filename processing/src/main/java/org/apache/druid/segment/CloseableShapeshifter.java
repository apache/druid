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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;

/**
 * A CloseableShapeshifter is an interface created to allow Segments to be used from {@link #as(Class)}, but also to
 * be able to ensure that any resource used by the object returned from the {@link #as(Class)} method have proper
 * management of their lifecycle.  This was initially introduced in order to make it possible for {@link Segment} to
 * become a {@link org.apache.druid.query.rowsandcols.RowsAndColumns} without needing to add extra close() methods to
 * {@link org.apache.druid.query.rowsandcols.RowsAndColumns}.
 */
public interface CloseableShapeshifter extends Closeable
{
  /**
   * Asks the Object to return itself as a concrete implementation of a specific interface.  The interface
   * asked for will tend to be a semantically-meaningful interface.
   *
   * @param clazz A class object representing the interface that the calling code wants a concrete implementation of
   * @param <T>   The interface that the calling code wants a concrete implementation of
   * @return A concrete implementation of the interface, or null if there is no meaningful optimization to be had
   * through a local implementation of the interface.
   */
  @Nullable
  <T> T as(@Nonnull Class<T> clazz);
}
