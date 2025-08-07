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

import java.io.Closeable;
import java.util.Optional;

/**
 * Interface to capture the pattern of providing tracked usage of some resource. Callers use {@link #acquireReference()}
 * to check out a reference to this resource as a {@link Closeable} which releases the reference upon close.
 * Implementations of this class are expected to be thread-safe, but the object returned by {@link #acquireReference()}
 * has no such guarantees, so check with the implementation.
 * <p>
 * For ease of implementation, implementors may extend {@link ReferenceCountingCloseableObject}, using the
 * {@link Closeable} from {@link ReferenceCountingCloseableObject#incrementReferenceAndDecrementOnceCloseable()} in the
 * close method of the object returned by {@link #acquireReference()}.
 */
public interface ReferenceCountedObjectProvider<T extends Closeable>
{
  /**
   * This method increments a reference count and provides a {@link Closeable} that decrements the reference count when
   * closed. The object returned by this method may or may not be thread-safe, it is best to check with the
   * implementation.
   * <p>
   * IMPORTANT NOTE: to fulfill the contract of this method, implementors must return a closeable to indicate that the
   * reference can be acquired, even if there is nothing to close. Implementors should ideally avoid allowing this
   * method to throw exceptions, but callers are responsible for closing the returned object should any operations on
   * it throw exceptions.
   * <p>
   * For callers: if this method returns non-empty, IT MUST BE CLOSED, else reference counts can potentially leak.
   */
  Optional<T> acquireReference();
}
