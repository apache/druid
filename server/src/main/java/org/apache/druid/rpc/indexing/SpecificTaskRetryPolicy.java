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

package org.apache.druid.rpc.indexing;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.segment.realtime.firehose.ChatHandlerResource;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

/**
 * Retry policy for tasks. Meant to be used together with {@link SpecificTaskServiceLocator}.
 *
 * Returns true from {@link #retryHttpResponse} when encountering an HTTP 400 or HTTP 404 with a
 * {@link ChatHandlerResource#TASK_ID_HEADER} header for a different task. This can happen when a task is suspended and
 * then later restored in a different location, and then some *other* task reuses its old port. This task-mismatch
 * scenario is retried indefinitely, since we expect that the {@link SpecificTaskServiceLocator} will update the
 * location at some point.
 */
public class SpecificTaskRetryPolicy implements ServiceRetryPolicy
{
  private final String taskId;
  private final ServiceRetryPolicy baseRetryPolicy;

  public SpecificTaskRetryPolicy(final String taskId, final ServiceRetryPolicy baseRetryPolicy)
  {
    this.taskId = Preconditions.checkNotNull(taskId, "taskId");
    this.baseRetryPolicy = Preconditions.checkNotNull(baseRetryPolicy, "baseRetryPolicy");
  }

  @Override
  public long maxAttempts()
  {
    return baseRetryPolicy.maxAttempts();
  }

  @Override
  public long minWaitMillis()
  {
    return baseRetryPolicy.minWaitMillis();
  }

  @Override
  public long maxWaitMillis()
  {
    return baseRetryPolicy.maxWaitMillis();
  }

  @Override
  public boolean retryHttpResponse(final HttpResponse response)
  {
    return baseRetryPolicy.retryHttpResponse(response) || isTaskMismatch(response);
  }

  @Override
  public boolean retryThrowable(final Throwable t)
  {
    return StandardRetryPolicy.unlimited().retryThrowable(t);
  }

  private boolean isTaskMismatch(final HttpResponse response)
  {
    // See class-level javadocs for details on why we do this.
    if (response.getStatus().equals(HttpResponseStatus.NOT_FOUND)
        || response.getStatus().equals(HttpResponseStatus.BAD_REQUEST)) {
      final String headerTaskId = StringUtils.urlDecode(response.headers().get(ChatHandlerResource.TASK_ID_HEADER));
      return headerTaskId != null && !headerTaskId.equals(taskId);
    } else {
      return false;
    }
  }
}
