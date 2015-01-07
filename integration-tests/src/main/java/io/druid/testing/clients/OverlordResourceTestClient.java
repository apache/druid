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

package io.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.StatusResponseHandler;
import com.metamx.http.client.response.StatusResponseHolder;
import io.druid.guice.annotations.Global;
import io.druid.indexing.common.TaskStatus;
import io.druid.indexing.common.task.Task;
import io.druid.testing.IntegrationTestingConfig;
import io.druid.testing.utils.RetryUtil;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.net.URL;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

public class OverlordResourceTestClient
{
  private final static Logger LOG = new Logger(OverlordResourceTestClient.class);
  private final ObjectMapper jsonMapper;
  private final HttpClient httpClient;
  private final String indexer;
  private final StatusResponseHandler responseHandler;

  @Inject
  OverlordResourceTestClient(
      ObjectMapper jsonMapper,
      @Global HttpClient httpClient, IntegrationTestingConfig config
  )
  {
    this.jsonMapper = jsonMapper;
    this.httpClient = httpClient;
    this.indexer = config.getIndexerHost();
    this.responseHandler = new StatusResponseHandler(Charsets.UTF_8);
  }

  private String getIndexerURL()
  {
    return String.format(
        "http://%s/druid/indexer/v1/",
        indexer
    );
  }

  public String submitTask(Task task)
  {
    try {
      return submitTask(this.jsonMapper.writeValueAsString(task));
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public String submitTask(String task)
  {
    try {
      StatusResponseHolder response = httpClient.post(new URL(getIndexerURL() + "task"))
                                                .setContent(
                                                    "application/json",
                                                    task.getBytes()
                                                )
                                                .go(responseHandler)
                                                .get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE(
            "Error while submitting task to indexer response [%s %s]",
            response.getStatus(),
            response.getContent()
        );
      }
      Map<String, String> responseData = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, String>>()
          {
          }
      );
      String taskID = responseData.get("task");
      LOG.info("Submitted task with TaskID[%s]", taskID);
      return taskID;
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public TaskStatus.Status getTaskStatus(String taskID)
  {
    try {
      StatusResponseHolder response = makeRequest(
          String.format(
              "%stask/%s/status",
              getIndexerURL(),
              URLEncoder.encode(taskID, "UTF-8")
          )
      );

      LOG.info("Index status response" + response.getContent());
      Map<String, Object> responseData = jsonMapper.readValue(
          response.getContent(), new TypeReference<Map<String, Object>>()
          {
          }
      );
      //TODO: figure out a better way to parse the response...
      String status = (String) ((Map) responseData.get("status")).get("status");
      return TaskStatus.Status.valueOf(status);
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public List<TaskResponseObject> getRunningTasks()
  {
    return getTasks("runningTasks");
  }

  public List<TaskResponseObject> getWaitingTasks()
  {
    return getTasks("waitingTasks");
  }

  public List<TaskResponseObject> getPendingTasks()
  {
    return getTasks("pendingTasks");
  }

  private List<TaskResponseObject> getTasks(String identifier)
  {
    try {
      StatusResponseHolder response = makeRequest(
          String.format("%s%s", getIndexerURL(), identifier)
      );
      LOG.info("Tasks %s response %s", identifier, response.getContent());
      return jsonMapper.readValue(
          response.getContent(), new TypeReference<List<TaskResponseObject>>()
          {
          }
      );
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public void waitUntilTaskCompletes(final String taskID)
  {
    RetryUtil.retryUntil(
        new Callable<Boolean>()
        {
          @Override
          public Boolean call() throws Exception
          {
            TaskStatus.Status status = getTaskStatus(taskID);
            if (status == TaskStatus.Status.FAILED) {
              throw new ISE("Indexer task FAILED");
            }
            return status == TaskStatus.Status.SUCCESS;
          }
        },
        true,
        60000,
        10,
        "Index Task to complete"
    );
  }

  private StatusResponseHolder makeRequest(String url)
  {
    try {
      StatusResponseHolder response = this.httpClient
          .get(new URL(url))
          .go(responseHandler)
          .get();
      if (!response.getStatus().equals(HttpResponseStatus.OK)) {
        throw new ISE("Error while making request to indexer [%s %s]", response.getStatus(), response.getContent());
      }
      return response;
    }
    catch (Exception e) {
      LOG.error(e, "Exception while sending request");
      throw Throwables.propagate(e);
    }
  }

}
