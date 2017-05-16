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

package io.druid.segment.data;

import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;

public interface Indexed<T> extends Iterable<T>, HotLoopCallee
{
  Class<? extends T> getClazz();

  int size();

  @CalledFromHotLoop
  T get(int index);

  /**
   * Returns the index of "value" in this Indexed object, or a negative number if the value is not present.
   * The negative number is not guaranteed to be any particular number. Subclasses may tighten this contract
   * (GenericIndexed does this).
   *
   * @param value value to search for
   *
   * @return index of value, or a negative number
   */
  int indexOf(T value);
}
