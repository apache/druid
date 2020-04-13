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

package org.apache.druid.java.util.emitter.core;

import java.io.Closeable;
import java.io.Flushable;
import java.io.IOException;

/**
 */
public interface Emitter extends Closeable, Flushable
{
  void start();

  /**
   * Emit an event. This method must not throw exceptions or block.
   *
   * If an implementation receives too many events and internal queues fill up, it should drop events rather than
   * blocking or consuming excessive memory.
   *
   * If an implementation receives input it considers to be invalid, or has an internal problem, it should deal with
   * that by logging a warning rather than throwing an exception. Implementations that log warnings should consider
   * throttling warnings to avoid excessive logs, since a busy Druid cluster can emit a high volume of metric events.
   */
  void emit(Event event);

  @Override
  void flush() throws IOException;

  @Override
  void close() throws IOException;
}
