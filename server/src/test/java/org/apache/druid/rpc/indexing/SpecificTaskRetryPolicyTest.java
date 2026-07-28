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

import io.netty.handler.codec.http.DefaultHttpResponse;
import io.netty.handler.codec.http.HttpResponse;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;
import org.apache.druid.rpc.ServiceRetryPolicy;
import org.apache.druid.rpc.StandardRetryPolicy;
import org.apache.druid.segment.realtime.ChatHandlerResource;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.Nullable;

/**
 * Covers the task-mismatch detection described in the {@link SpecificTaskRetryPolicy} class javadoc: a 400 or 404
 * carrying another task's id means this task moved and something else took its port, so the request should be retried
 * rather than surfaced as a failure.
 */
public class SpecificTaskRetryPolicyTest
{
  private static final String TASK_ID = "taskId";

  private static SpecificTaskRetryPolicy policy(final ServiceRetryPolicy baseRetryPolicy)
  {
    return new SpecificTaskRetryPolicy(TASK_ID, baseRetryPolicy);
  }

  private static HttpResponse response(final HttpResponseStatus status, @Nullable final String taskIdHeader)
  {
    final HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    if (taskIdHeader != null) {
      response.headers().set(ChatHandlerResource.TASK_ID_HEADER, taskIdHeader);
    }
    return response;
  }

  @Test
  public void testRetriesNotFoundFromAnotherTask()
  {
    Assert.assertTrue(
        policy(StandardRetryPolicy.noRetries())
            .retryHttpResponse(response(HttpResponseStatus.NOT_FOUND, "someOtherTask"))
    );
  }

  @Test
  public void testRetriesBadRequestFromAnotherTask()
  {
    Assert.assertTrue(
        policy(StandardRetryPolicy.noRetries())
            .retryHttpResponse(response(HttpResponseStatus.BAD_REQUEST, "someOtherTask"))
    );
  }

  @Test
  public void testDoesNotRetryNotFoundFromOurOwnTask()
  {
    Assert.assertFalse(
        policy(StandardRetryPolicy.noRetries())
            .retryHttpResponse(response(HttpResponseStatus.NOT_FOUND, TASK_ID))
    );
  }

  /**
   * Task ids are sent URL-encoded, so a match has to be recognised after decoding.
   */
  @Test
  public void testDoesNotRetryUrlEncodedIdFromOurOwnTask()
  {
    final SpecificTaskRetryPolicy retryPolicy =
        new SpecificTaskRetryPolicy("task with spaces", StandardRetryPolicy.noRetries());

    Assert.assertFalse(
        retryPolicy.retryHttpResponse(response(HttpResponseStatus.NOT_FOUND, "task%20with%20spaces"))
    );
  }

  @Test
  public void testDoesNotRetryWhenTaskIdHeaderIsAbsent()
  {
    Assert.assertFalse(
        policy(StandardRetryPolicy.noRetries()).retryHttpResponse(response(HttpResponseStatus.NOT_FOUND, null))
    );
  }

  /**
   * A status the mismatch check does not care about must be left entirely to the base policy.
   */
  @Test
  public void testIgnoresOtherStatusesEvenFromAnotherTask()
  {
    Assert.assertFalse(
        policy(StandardRetryPolicy.noRetries())
            .retryHttpResponse(response(HttpResponseStatus.OK, "someOtherTask"))
    );
  }

  @Test
  public void testStillDefersToBaseRetryPolicy()
  {
    Assert.assertTrue(
        policy(StandardRetryPolicy.unlimited())
            .retryHttpResponse(response(HttpResponseStatus.SERVICE_UNAVAILABLE, null))
    );
  }
}
