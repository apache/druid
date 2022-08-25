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

package org.apache.druid.frame.processor;

import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.java.util.common.StringUtils;

/**
 * Exception that is conventionally thrown by workers when they call
 * {@link FrameWriter#addSelection} and it returns false on an empty frame, or in
 * a situation where allocating a new frame is impractical.
 */
public class FrameRowTooLargeException extends RuntimeException
{
  private final long maxFrameSize;

  public FrameRowTooLargeException(final long maxFrameSize)
  {
    super(StringUtils.format("Row too large to add to frame (max frame size = %,d)", maxFrameSize));
    this.maxFrameSize = maxFrameSize;
  }

  public long getMaxFrameSize()
  {
    return maxFrameSize;
  }
}
