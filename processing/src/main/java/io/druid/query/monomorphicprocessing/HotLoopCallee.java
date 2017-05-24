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

/**
 * Marker interface for abstractions, which are called from hot loops during query processing. Some of the methods of
 * interfaces extending HotLoopCallee should be annotated with {@link CalledFromHotLoop}.
 */
public interface HotLoopCallee
{
  /**
   * Implementations of this method should call {@code inspector.visit()} with all fields of this class, which meet two
   * conditions:
   *  1. They are used in methods of this class, annotated with {@link CalledFromHotLoop}
   *  2. They are either:
   *     a. Nullable objects
   *     b. Instances of HotLoopCallee
   *     c. Objects, which don't always have a specific class in runtime. For example, a field of type {@link
   *        java.util.Set} could be {@link java.util.HashSet} or {@link java.util.TreeSet} in runtime, depending on how
   *        this instance (the instance on which inspectRuntimeShape() is called) is configured.
   *     d. ByteBuffer or similar objects, where byte order matters
   *     e. boolean flags, affecting branch taking
   *     f. Arrays of objects, meeting any of conditions a-e.
   */
  void inspectRuntimeShape(RuntimeShapeInspector inspector);
}
