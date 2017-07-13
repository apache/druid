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

package io.druid.query.groupby.resource;

import io.druid.collections.ResourceHolder;
import io.druid.java.util.common.logger.Logger;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

/**
 * This class contains resources required for a groupBy query execution.
 * Currently, it contains only merge buffers, but any additional resources can be added in the future.
 */
public class GroupByQueryResource implements Closeable
{
  private static final Logger log = new Logger(GroupByQueryResource.class);

  private final ResourceHolder<List<ByteBuffer>> mergeBuffersHolder;
  private final Deque<ByteBuffer> mergeBuffers;

  public GroupByQueryResource()
  {
    this.mergeBuffersHolder = null;
    this.mergeBuffers = new ArrayDeque<>();
  }

  public GroupByQueryResource(ResourceHolder<List<ByteBuffer>> mergeBuffersHolder)
  {
    this.mergeBuffersHolder = mergeBuffersHolder;
    this.mergeBuffers = new ArrayDeque<>(mergeBuffersHolder.get());
  }

  /**
   * Get a merge buffer from the pre-acquired resources.
   *
   * @return a resource holder containing a merge buffer
   *
   * @throws IllegalStateException if this resource is initialized with empty merge buffers, or
   *                               there isn't any available merge buffers
   */
  public ResourceHolder<ByteBuffer> getMergeBuffer()
  {
    final ByteBuffer buffer = mergeBuffers.pop();
    return new ResourceHolder<ByteBuffer>()
    {
      @Override
      public ByteBuffer get()
      {
        return buffer;
      }

      @Override
      public void close()
      {
        mergeBuffers.add(buffer);
      }
    };
  }

  @Override
  public void close()
  {
    if (mergeBuffersHolder != null) {
      if (mergeBuffers.size() != mergeBuffersHolder.get().size()) {
        log.warn("%d resources are not returned yet", mergeBuffersHolder.get().size() - mergeBuffers.size());
      }
      mergeBuffersHolder.close();
    }
  }
}
