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

package io.druid.output;

import io.druid.java.util.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;

/**
 * OutputMedium manages the resources of a bunch of {@link OutputBytes} objects of the same kind, created by calling
 * {@link #makeOutputBytes()} on the OutputMedium object. When OutputMedium is closed, all child OutputBytes couldn't
 * be used anymore.
 */
public interface OutputMedium extends Closeable
{
  /**
   * Creates a new empty {@link OutputBytes}, attached to this OutputMedium. When this OutputMedium is closed, the
   * returned OutputBytes couldn't be used anymore.
   */
  OutputBytes makeOutputBytes() throws IOException;

  /**
   * Returns a closer of this OutputMedium, which is closed in this OutputMedium's close() method. Could be used to
   * "attach" some random resources to this OutputMedium, to be closed at the same time.
   */
  Closer getCloser();
}
