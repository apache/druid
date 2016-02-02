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

package io.druid.server.coordinator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.metamx.common.ISE;
import com.metamx.emitter.EmittingLogger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.server.coordination.DataSegmentChangeRequest;
import io.druid.server.coordination.SegmentChangeRequestNoop;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.utils.ZKPaths;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class HttpLoadQueuePeon extends LoadQueuePeon
{
  private static final EmittingLogger log = new EmittingLogger(HttpLoadQueuePeon.class);

  private final HttpClient httpClient;
  private final String baseURL;
  private final ObjectMapper jsonMapper;
  private final ScheduledExecutorService processingExecutor;
  private final DruidCoordinatorConfig config;
  private final StatusResponseHandler responseHandler;
  private final Map<DataSegmentChangeRequest, Runnable> pendingRequests = Maps.newConcurrentMap();

  HttpLoadQueuePeon(
      final HttpClient httpClient,
      String baseURL,
      ObjectMapper jsonMapper,
      ScheduledExecutorService processingExecutor,
      ExecutorService callbackExecutor,
      DruidCoordinatorConfig config
  )
  {
    super(baseURL, processingExecutor, callbackExecutor);
    this.httpClient = httpClient;
    this.baseURL = baseURL;
    this.jsonMapper = jsonMapper;
    this.processingExecutor = processingExecutor;
    this.config = config;
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
    processingExecutor.scheduleAtFixedRate(
        new Runnable()
        {
          @Override
          public void run()
          {
            try {
              final List<DataSegmentChangeRequest> remotePendingRequests = getRemotePendingRequests();
              Map<DataSegmentChangeRequest, Runnable> completedRequests = Maps.newHashMap(Maps.filterKeys(
                  pendingRequests,
                  new Predicate<DataSegmentChangeRequest>()
                  {
                    @Override
                    public boolean apply(
                        DataSegmentChangeRequest request
                    )
                    {
                      return !remotePendingRequests.contains(request);
                    }
                  }
              ));
              for (Map.Entry<DataSegmentChangeRequest, Runnable> completedRequest : completedRequests.entrySet()) {
                completedRequest.getValue().run();
                pendingRequests.remove(completedRequest.getKey());
              }
            }
            catch (Exception e) {
              log.error("Exception while fetching remote requests, Will retry again..", e);
            }
          }
        },
        config.getLoadStatusPollDuration().getMillis(),
        config.getLoadStatusPollDuration().getMillis(),
        TimeUnit.MILLISECONDS
    );

  }

  private List<DataSegmentChangeRequest> getRemotePendingRequests() throws Exception
  {
    StatusResponseHolder response = httpClient.go(
        new Request(
            HttpMethod.GET, new URL(String.format("%s/pendingRequests", baseURL))
        ),
        responseHandler
    ).get();
    if (!response.getStatus().equals(HttpResponseStatus.OK)) {
      throw new ISE(
          "Error while fetching pending Requests on url[%s] status[%s] content[%s]",
          baseURL,
          response.getStatus(),
          response.getContent()
      );
    }
    return jsonMapper.readValue(
        response.getContent(),
        new TypeReference<List<DataSegmentChangeRequest>>()
        {
        }
    );
  }


  @Override
  void processHolder(final SegmentHolder holder)
  {
    try {
      StatusResponseHolder response = httpClient.go(
          new Request(
              HttpMethod.POST, new URL(String.format("%s/processRequest", baseURL))
          ).setContent(
              MediaType.APPLICATION_JSON,
              jsonMapper.writeValueAsBytes(holder.getChangeRequest())
          ),
          responseHandler
      ).get();

      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while processing SegmentChangeRequest on url[%s] status[%s] content[%s]",
            baseURL,
            response.getStatus(),
            response.getContent()
        );
      }
      // Add to pending Requests will be called when the request completes.
      pendingRequests.put(holder.getChangeRequest(), new Runnable()
      {
        @Override
        public void run()
        {
          actionCompleted(holder);
        }
      });

      // Schedule timeout check.
      processingExecutor.schedule(
          new Runnable()
          {
            @Override
            public void run()
            {
              if (pendingRequests.remove(holder.getChangeRequest()) != null) {
                failAssign(holder, new ISE("%s was never processed! Failing this operation!", holder));
              }
            }
          },
          config.getLoadTimeoutDelay().getMillis(),
          TimeUnit.MILLISECONDS
      );


    }
    catch (Exception e) {
      failAssign(holder, e);
    }
  }


}
