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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.ToStringResponseHandler;
import io.druid.client.selector.Server;
import io.druid.curator.discovery.ServerDiscoverySelector;
import io.druid.indexing.common.RetryPolicy;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.task.Task;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final HttpClient httpClient;
  private final ServerDiscoverySelector selector;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(
      Task task,
      HttpClient httpClient,
      ServerDiscoverySelector selector,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.task = task;
    this.httpClient = httpClient;
    this.selector = selector;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    byte[] dataToSend = jsonMapper.writeValueAsBytes(new TaskActionHolder(task, taskAction));

    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();

    while (true) {
      try {
        final URI serviceUri;
        try {
          serviceUri = getServiceUri();
        }
        catch (Exception e) {
          throw new IOException("Failed to locate service uri", e);
        }

        final String response;

        log.info("Submitting action for task[%s] to coordinator[%s]: %s", task.getId(), serviceUri, taskAction);

        try {
          response = httpClient.post(serviceUri.toURL())
                               .setContent("application/json", dataToSend)
                               .go(new ToStringResponseHandler(Charsets.UTF_8))
                               .get();
        }
        catch (Exception e) {
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          throw Throwables.propagate(e);
        }

        final Map<String, Object> responseDict = jsonMapper.readValue(
            response, new TypeReference<Map<String, Object>>()
        {
        }
        );

        return jsonMapper.convertValue(responseDict.get("result"), taskAction.getReturnTypeReference());
      }
      catch (IOException e) {
        log.warn(e, "Exception submitting action for task[%s]", task.getId());

        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (delay == null) {
          throw e;
        } else {
          try {
            final long sleepTime = delay.getMillis();
            log.info("Will try again in [%s].", new Duration(sleepTime).toString());
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            throw Throwables.propagate(e2);
          }
        }
      }
    }
  }

  private URI getServiceUri() throws Exception
  {
    final Server instance = selector.pick();
    if (instance == null) {
      throw new ISE("Cannot find instance of indexer to talk to!");
    }

    return new URI(
        instance.getScheme(),
        null,
        instance.getHost(),
        instance.getPort(),
        "/druid/indexer/v1/action",
        null,
        null
    );
  }
}
