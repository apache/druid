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

package org.apache.druid.msq.exec;

import org.apache.druid.collections.NonBlockingPool;
import org.apache.druid.collections.QueueNonBlockingPool;
import org.apache.druid.frame.processor.Bouncer;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Holds a processing buffer pool, and a {@link Bouncer} used to limit concurrent access to the buffer pool.
 * Thread-safe. Used by {@link RunWorkOrder} by way of {@link FrameContext#processingBuffers()}.
 */
public class ProcessingBuffers
{
  private final NonBlockingPool<ByteBuffer> bufferPool;
  private final Bouncer bouncer;

  public ProcessingBuffers(final NonBlockingPool<ByteBuffer> bufferPool, final Bouncer bouncer)
  {
    this.bufferPool = bufferPool;
    this.bouncer = bouncer;
  }

  public static ProcessingBuffers fromCollection(final Collection<ByteBuffer> bufferPool)
  {
    final BlockingQueue<ByteBuffer> queue = new ArrayBlockingQueue<>(bufferPool.size());
    queue.addAll(bufferPool);
    return new ProcessingBuffers(new QueueNonBlockingPool<>(queue), new Bouncer(queue.size()));
  }

  public NonBlockingPool<ByteBuffer> getBufferPool()
  {
    return bufferPool;
  }

  public Bouncer getBouncer()
  {
    return bouncer;
  }
}
