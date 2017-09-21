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

import io.druid.query.monomorphicprocessing.CalledFromHotLoop;
import io.druid.query.monomorphicprocessing.HotLoopCallee;

/**
 * An object that gets a metric value.  Metric values are always floats and there is an assumption that the
 * FloatColumnSelector has a handle onto some other stateful object (e.g. an Offset) which is changing between calls
 * to get() (though, that doesn't have to be the case if you always want the same value...).
 */
public interface FloatColumnSelector extends ColumnValueSelector<Float>, HotLoopCallee
{
  @CalledFromHotLoop
  @Override
  float getFloat();

  /**
   * @deprecated This method is marked as deprecated in FloatColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of FloatColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @CalledFromHotLoop
  @Override
  default double getDouble()
  {
    return getFloat();
  }

  /**
   * @deprecated This method is marked as deprecated in FloatColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of FloatColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @CalledFromHotLoop
  @Override
  default long getLong()
  {
    return (long) getFloat();
  }

  /**
   * @deprecated This method is marked as deprecated in FloatColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of FloatColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default Float getObject()
  {
    return getFloat();
  }

  /**
   * @deprecated This method is marked as deprecated in FloatColumnSelector to minimize the probability of accidential
   * calling. "Polymorphism" of FloatColumnSelector should be used only when operating on {@link ColumnValueSelector}
   * objects.
   */
  @Deprecated
  @Override
  default Class<Float> classOfObject()
  {
    return Float.class;
  }
}
