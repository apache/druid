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

package io.druid.offheap;

import com.google.common.base.Supplier;

import io.druid.java.util.common.logger.Logger;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class OffheapBufferGenerator implements Supplier<ByteBuffer>
{
  private static final Logger log = new Logger(OffheapBufferGenerator.class);

  private final String description;
  private final int computationBufferSize;
  private final AtomicLong count = new AtomicLong(0);

  public OffheapBufferGenerator(String description, int computationBufferSize)
  {
    this.description = description;
    this.computationBufferSize = computationBufferSize;
  }

  @Override
  public ByteBuffer get()
  {
    log.info(
        "Allocating new %s buffer[%,d] of size[%,d]",
        description,
        count.getAndIncrement(),
        computationBufferSize
    );

    return ByteBuffer.allocateDirect(computationBufferSize);
  }
}
