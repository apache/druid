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

package org.apache.druid.client;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.HttpResponseHandler;
import org.joda.time.Duration;

import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A test {@link HttpClient} that captures {@link Request}s and serves preloaded responses sequentially. If no future
 * has been {@link #enqueue(ListenableFuture) enqueued} when {@link #go(Request, HttpResponseHandler, Duration)} is
 * invoked, the client returns a pending future that never completes.
 *
 * <p>This is useful for tests that need deterministic control over request ordering and response timing.</p>
 */
public class QueuedTestHttpClient implements HttpClient
{
  private final List<Request> captured = new ArrayList<>();
  private final Deque<ListenableFuture<InputStream>> queue = new ArrayDeque<>();

  public void enqueue(ListenableFuture<InputStream> f)
  {
    queue.addLast(f);
  }

  public List<Request> getRequests()
  {
    return captured;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <Intermediate, Final> ListenableFuture<Final> go(
      Request request,
      HttpResponseHandler<Intermediate, Final> handler,
      Duration readTimeout
  )
  {
    captured.add(request);
    ListenableFuture<InputStream> f = queue.pollFirst();
    if (f == null) {
      f = SettableFuture.create(); // pending forever
    }
    return (ListenableFuture<Final>) f;
  }

  @Override
  public <Intermediate, Final> ListenableFuture<Final> go(Request request, HttpResponseHandler<Intermediate, Final> handler)
  {
    throw new UnsupportedOperationException("Use go(Request, HttpResponseHandler<Intermediate, Final>, Duration) instead)");
  }
}

