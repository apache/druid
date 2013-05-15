/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.druid.indexing.common.RetryPolicyFactory;
import com.metamx.http.client.HttpClient;
import org.apache.curator.x.discovery.ServiceProvider;

/**
 */
public class RemoteTaskActionClientFactory implements TaskActionClientFactory
{
  private final HttpClient httpClient;
  private final ServiceProvider serviceProvider;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;

  public RemoteTaskActionClientFactory(
      HttpClient httpClient,
      ServiceProvider serviceProvider,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.httpClient = httpClient;
    this.serviceProvider = serviceProvider;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public TaskActionClient create(Task task)
  {
    return new RemoteTaskActionClient(task, httpClient, serviceProvider, retryPolicyFactory, jsonMapper);
  }
}
