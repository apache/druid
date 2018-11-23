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

import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.io.Closer;

import java.io.IOException;

@VisibleForTesting
public final class OnHeapMemorySegmentWriteOutMedium implements SegmentWriteOutMedium
{
  private final Closer closer = Closer.create();

  @Override
  public WriteOutBytes makeWriteOutBytes()
  {
    return new HeapByteBufferWriteOutBytes();
  }

  @Override
  public Closer getCloser()
  {
    return closer;
  }

  @Override
  public void close() throws IOException
  {
    closer.close();
  }
}
