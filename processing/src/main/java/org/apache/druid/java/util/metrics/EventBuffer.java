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

package org.apache.druid.java.util.metrics;


import java.lang.reflect.Array;

// A fixed-size, thread-safe, append-only ring buffer for buffering events prior to emission.
// Events will overflow and wrap-around if buffer size is exceeded within a single emission window.
public class EventBuffer<Event>
{
  private final Event[] buffer;
  private final Class<Event> clazz;
  private final int capacity;
  private int back;
  private int size;

  public EventBuffer(Class<Event> clazz, int capacity)
  {
    this.clazz = clazz;
    this.buffer = (Event[]) Array.newInstance(clazz, capacity);
    this.capacity = capacity;
    this.back = 0;
  }

  public synchronized int getCapacity()
  {
    return capacity;
  }

  public synchronized int getSize()
  {
    return size;
  }

  public synchronized void push(Event event)
  {
    buffer[back] = event;
    back = (back + 1) % capacity;

    if (size < capacity) {
      ++size;
    }
  }

  public synchronized Event[] extract()
  {
    final Event[] finalEvents = (Event[]) Array.newInstance(clazz, size);
    System.arraycopy(buffer, 0, finalEvents, 0, size);
    size = back = 0;
    return finalEvents;
  }
}
