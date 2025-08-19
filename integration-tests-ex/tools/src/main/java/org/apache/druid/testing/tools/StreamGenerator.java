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

package org.apache.druid.testing.tools;

import org.joda.time.DateTime;

import java.util.List;

public interface StreamGenerator
{
  /**
   * Runs and returns the number of messages written.
   */
  long run(String streamTopic, StreamEventWriter streamEventWriter, int totalNumberOfSeconds);

  /**
   * Runs and returns the number of messages written.
   */
  long run(String streamTopic, StreamEventWriter streamEventWriter, int totalNumberOfSeconds, DateTime overrrideFirstEventTime);

  /**
   * Generates a list of byte arrays representing events for the given number of seconds.
   * The first event will be at the current time.
   *
   * @param totalNumberOfSeconds The total number of seconds to generate events for.
   * @return A list of byte arrays, each representing an event.
   */
  List<byte[]> generateEvents(int totalNumberOfSeconds);

  /**
   * Generates a list of byte arrays representing events for the given number of seconds,
   * starting from the specified override time.
   *
   * @param totalNumberOfSeconds The total number of seconds to generate events for.
   * @param overrideFirstEventTime The starting point for timestamps of events generated.
   * @return A list of byte arrays, each representing an event.
   */
  List<byte[]> generateEvents(int totalNumberOfSeconds, DateTime overrideFirstEventTime);
}
