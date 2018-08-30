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

import org.apache.druid.java.util.common.ByteBufferUtils;

import java.nio.ByteBuffer;

final class DirectByteBufferWriteOutBytes extends ByteBufferWriteOutBytes
{
  private boolean open = true;

  @Override
  protected ByteBuffer allocateBuffer()
  {
    return ByteBuffer.allocateDirect(BUFFER_SIZE);
  }

  @Override
  public boolean isOpen()
  {
    return open;
  }

  void free()
  {
    open = false;
    buffers.forEach(ByteBufferUtils::free);
    buffers.clear();
    headBufferIndex = -1;
    headBuffer = null;
    size = 0;
    capacity = 0;
  }
}
