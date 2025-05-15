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
 * Interface for "checking out" a {@link Closeable} object as a tracked a reference. This can power anything that
 * wishes to provide this pattern of acquiring and releasing of a reference to the object when it is closed.
 * <p>
 * For ease of implementation, {@link ReferenceCountingCloseableObject} can be used as a basis for implementing this
 * interface, using the {@link Closeable} from
 * {@link ReferenceCountingCloseableObject#incrementReferenceAndDecrementOnceCloseable()} in the close method of the
 * returned object of {@link #acquireReference()} to release the references when closing the object.
 */
public interface ReferenceCountedObjectProvider<T extends Closeable>
{
  /**
   * This method increments a reference count and provides a {@link Closeable} that decrements the reference count when
   * closed.
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
