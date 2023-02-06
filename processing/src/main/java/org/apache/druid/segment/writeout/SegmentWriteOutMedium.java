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

package org.apache.druid.segment.writeout;

import org.apache.druid.java.util.common.io.Closer;

import java.io.Closeable;
import java.io.IOException;

/**
 * SegmentWriteOutMedium is an umbrella "resource disposer" for temporary buffers (in the form of {@link WriteOutBytes},
 * obtained by calling {@link #makeWriteOutBytes()} on the SegmentWriteOutMedium instance), that are used during new Druid
 * segment creation, and other resources (see {@link #getCloser()}).
 *
 * When SegmentWriteOutMedium is closed, all child WriteOutBytes couldn't be used anymore.
 */
public interface SegmentWriteOutMedium extends Closeable
{
  /**
   * Creates a new empty {@link WriteOutBytes}, attached to this SegmentWriteOutMedium. When this SegmentWriteOutMedium is
   * closed, the returned WriteOutBytes couldn't be used anymore.
   */
  WriteOutBytes makeWriteOutBytes() throws IOException;

  /**
   * Creates a 'child' version of the {@link SegmentWriteOutMedium}, which can be optionally closed,
   * independent of this {@link SegmentWriteOutMedium} but otherwise shares the same configuration. This allows callers
   * using a shared {@link SegmentWriteOutMedium} but which control the complete lifecycle of the {@link WriteOutBytes}
   * which they require to free the backing resources when they are finished, rather than waiting until
   * {@link #close()} is called for this medium.
   *
   * The 'child' medium will be closed when {@link #close()} is called, if not called explicitly prior to closing this
   * medium.
   */
  SegmentWriteOutMedium makeChildWriteOutMedium() throws IOException;

  /**
   * Returns a closer of this SegmentWriteOutMedium, which is closed in this SegmentWriteOutMedium's close() method.
   * Could be used to "attach" some random resources to this SegmentWriteOutMedium, to be closed at the same time.
   */
  Closer getCloser();
}
