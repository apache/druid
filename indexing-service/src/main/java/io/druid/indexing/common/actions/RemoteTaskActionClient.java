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

package io.druid.indexing.common.actions;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Throwables;
import com.metamx.http.client.response.FullResponseHolder;
import io.druid.discovery.DruidLeaderClient;
import io.druid.indexing.common.RetryPolicy;
import io.druid.indexing.common.RetryPolicyFactory;
import io.druid.indexing.common.task.Task;
import io.druid.java.util.common.IOE;
import io.druid.java.util.common.jackson.JacksonUtils;
import io.druid.java.util.common.logger.Logger;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.Duration;

import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;
  private final DruidLeaderClient druidLeaderClient;
  private final Random random = new Random();

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(
      Task task,
      DruidLeaderClient druidLeaderClient,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.task = task;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
    this.druidLeaderClient = druidLeaderClient;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    byte[] dataToSend = jsonMapper.writeValueAsBytes(new TaskActionHolder(task, taskAction));

    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();

    while (true) {
      try {

        final FullResponseHolder fullResponseHolder;

        log.info("Submitting action for task[%s] to overlord: [%s].", task.getId(), taskAction);

        fullResponseHolder = druidLeaderClient.go(
            druidLeaderClient.makeRequest(HttpMethod.POST, "/druid/indexer/v1/action")
                             .setContent(MediaType.APPLICATION_JSON, dataToSend)
        );

        if (fullResponseHolder.getStatus().getCode() / 100 == 2) {
          final Map<String, Object> responseDict = jsonMapper.readValue(
              fullResponseHolder.getContent(),
              JacksonUtils.TYPE_REFERENCE_MAP_STRING_OBJECT
          );
          return jsonMapper.convertValue(responseDict.get("result"), taskAction.getReturnTypeReference());
        } else {
          // Want to retry, so throw an IOException.
          throw new IOE(
              "Scary HTTP status returned: %s. Check your overlord logs for exceptions.",
              fullResponseHolder.getStatus()
          );
        }
      }
      catch (IOException | ChannelException e) {
        log.warn(e, "Exception submitting action for task[%s]", task.getId());

        final Duration delay = retryPolicy.getAndIncrementRetryDelay();
        if (delay == null) {
          throw e;
        } else {
          try {
            final long sleepTime = jitter(delay.getMillis());
            log.info("Will try again in [%s].", new Duration(sleepTime).toString());
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            throw Throwables.propagate(e2);
          }
        }
      }
      catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private long jitter(long input)
  {
    final double jitter = random.nextGaussian() * input / 4.0;
    long retval = input + (long) jitter;
    return retval < 0 ? 0 : retval;
  }
}
