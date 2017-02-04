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

package io.druid.query.monomorphicprocessing;

import javax.annotation.Nullable;

/**
 * @see HotLoopCallee#inspectRuntimeShape(RuntimeShapeInspector)
 */
public interface RuntimeShapeInspector
{
  void visit(String fieldName, @Nullable HotLoopCallee value);

  void visit(String fieldName, @Nullable Object value);

  <T> void visit(String fieldName, T[] values);

  void visit(String flagName, boolean flagValue);

  /**
   * To be called from {@link HotLoopCallee#inspectRuntimeShape(RuntimeShapeInspector)} with something, that is
   * important to ensure monomorphism and predictable branch taking in hot loops, but doesn't apply to other visit()
   * methods in RuntimeShapeInspector. For example, {@link io.druid.segment.BitmapOffset#inspectRuntimeShape} reports
   * bitmap population via this method, to ensure predictable branch taking inside Bitmap's iterators.
   */
  void visit(String key, String runtimeShape);
}
