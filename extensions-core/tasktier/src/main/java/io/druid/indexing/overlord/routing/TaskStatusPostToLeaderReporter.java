/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Metamarkets licenses this file
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

package io.druid.indexing.overlord.routing;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.TaskStatus;
import io.druid.java.util.common.ISE;
import io.druid.java.util.common.logger.Logger;
import io.druid.server.DruidNode;
import org.apache.curator.x.discovery.ServiceDiscovery;
import org.apache.curator.x.discovery.ServiceInstance;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.joda.time.Duration;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

/**
 * A TaskStatusReporter which looks for a tier and reports with a POST.
 * It assumes the service will have a redirect if it is not the current leader.
 */
public class TaskStatusPostToLeaderReporter implements TaskStatusReporter
{
  private static final Logger log = new Logger(TaskStatusPostToLeaderReporter.class);
  private final HttpClient httpClient;
  private final ServiceDiscovery<Void> discovery;
  private final String upstreamService;

  @Inject
  public TaskStatusPostToLeaderReporter(
      @Global HttpClient httpClient,
      ServiceDiscovery<Void> discovery,
      @Named(TaskTierModule.UPSTREAM_SERVICE_NAME_CONSTANT_KEY) String upstreamService
  )
  {
    this.httpClient = httpClient;
    this.discovery = discovery;
    this.upstreamService = upstreamService;
  }

  @Override
  public boolean reportStatus(TaskStatus status)
  {
    final Collection<ServiceInstance<Void>> overlords;
    try {
      overlords = discovery.queryForInstances(upstreamService);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }

    if (overlords.isEmpty()) {
      throw new ISE("No overlords found for service [%s]", upstreamService);
    }

    final ServiceInstance<Void> overlord = Iterables.getFirst(overlords, null);
    if (overlord == null) {
      throw new ISE("Overlords not empty but had no entries?");
    }
    final DruidNode node = new DruidNode("ignored", overlord.getAddress(), overlord.getPort());
    try {
      final URL url = TaskStatusPostToLeaderListenerResource.makeReportUrl(node);
      final StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST,
              url
          ), new StatusResponseHandler(Charsets.UTF_8),
          Duration.millis(60_000) // TODO: make this configurable
      ).get();
      log.debug("Received [%s] for reporting status [%s] to [%s]", response, status, url);
      if (response.getStatus().equals(HttpResponseStatus.ACCEPTED)) {
        return true;
      }
      if (response.getStatus().equals(HttpResponseStatus.SERVICE_UNAVAILABLE)) {
        return false;
      }
      throw new ISE("Unknown response [%s] when submitting [%s] to [%s]", response, status, url);
    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw Throwables.propagate(e);
    }
    catch (ExecutionException | MalformedURLException e) {
      throw Throwables.propagate(e);
    }
  }
}
