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

package org.apache.druid.msq.input;

import com.fasterxml.jackson.annotation.JsonTypeInfo;

/**
 * Slice of an {@link InputSpec} assigned to a particular worker.
 *
 * On the controller, these are produced using {@link InputSpecSlicer}. On workers, these are read
 * using {@link InputSliceReader}.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
public interface InputSlice
{
  /**
   * Returns the number of files contained within this split. This is the same number that would be added to
   * {@link org.apache.druid.msq.counters.CounterTracker} on full iteration through {@link InputSliceReader#attach}.
   *
   * May be zero for some kinds of slices, even if they contain data, if the input is not file-based.
   */
  int fileCount();
}
