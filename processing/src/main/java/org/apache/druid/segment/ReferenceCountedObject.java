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

import org.apache.druid.java.util.common.io.Closer;

import java.io.Closeable;
import java.util.Optional;

/**
 * Interface for an object that may have a reference acquired in the form of a {@link Closeable}. This is intended to be
 * used with an implementation of {@link ReferenceCountingCloseableObject}, or anything else that wishes to provide
 * a method to account for the acquire and release of a reference to the object.
 */
public interface ReferenceCountedObject
{
  /**
   * This method is expected to increment a reference count and provide a {@link Closer} that decrements the reference
   * count when closed. This is likely just a wrapper around
   * {@link ReferenceCountingCloseableObject#referenceResources()}, but may also include any other associated references
   * which should be incremente when this method is called, and decremented/released by the closer.
   */
  Optional<Closeable> acquireReferences();
}
