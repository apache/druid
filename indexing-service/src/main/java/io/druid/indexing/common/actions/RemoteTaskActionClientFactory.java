/*
 * Druid - a distributed column store.
 * Copyright (C) 2012, 2013  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import com.metamx.http.client.HttpClient;
import io.druid.client.indexing.IndexingService;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.task.Task;

/**
 */
public class RemoteTaskActionClientFactory implements TaskActionClientFactory
{
  private final HttpClient httpClient;
  private final ServerDiscoverySelector selector;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;

  @Inject
  public RemoteTaskActionClientFactory(
      @Global HttpClient httpClient,
      @IndexingService ServerDiscoverySelector selector,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.httpClient = httpClient;
    this.selector = selector;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TaskActionClient create(Task task)
  {
    return new RemoteTaskActionClient(task, httpClient, selector, retryPolicyFactory, jsonMapper);
  }
}
