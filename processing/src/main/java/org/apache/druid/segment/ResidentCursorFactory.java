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
 * A {@link CursorFactory} whose {@link #makeCursorHolder} never blocks on I/O, i.e. a fully-resident / in-memory
 * source with no on-demand loading. Implementing this interface, rather than {@link CursorFactory} directly, declares
 * that intent and supplies the trivial {@link #makeCursorHolderAsync} implementation: the holder is built synchronously
 * and returned already completed.
 * <p>
 * Factories backed by, or wrapping, an on-demand source must implement {@link CursorFactory} directly and provide a
 * real {@link CursorFactory#makeCursorHolderAsync} that builds/awaits the holder asynchronously so they never block the
 * calling thread.
 */
public interface ResidentCursorFactory extends CursorFactory
{
  @Override
  default AsyncCursorHolder makeCursorHolderAsync(CursorBuildSpec spec)
  {
    return AsyncCursorHolder.completed(makeCursorHolder(spec));
  }
}
