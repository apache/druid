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

package org.apache.druid.indexing.pubsub;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.IndexTaskClient;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StringFullResponseHolder;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.ConnectException;

public class PubsubIndexTaskClient extends IndexTaskClient
{
  private static final Logger log = new Logger(PubsubIndexTaskClient.class);

  PubsubIndexTaskClient(
      HttpClient httpClient,
      ObjectMapper jsonMapper,
      TaskInfoProvider taskInfoProvider,
      String dataSource,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  )
  {
    super(
        httpClient,
        jsonMapper,
        taskInfoProvider,
        httpTimeout,
        dataSource,
        numThreads,
        numRetries
    );
  }

  public boolean stop(final String id, final boolean publish)
  {
    log.debug("Stop task[%s] publish[%s]", id, publish);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "stop",
          publish ? "publish=true" : null,
          true
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException e) {
      return false;
    }
    catch (TaskNotRunnableException e) {
      log.info("Task [%s] couldn't be stopped because it is no longer running", id);
      return true;
    }
    catch (Exception e) {
      log.warn(e, "Exception while stopping task [%s]", id);
      return false;
    }
  }


  public boolean resume(final String id)
  {
    log.debug("Resume task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "resume",
          null,
          true
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException | IOException e) {
      log.warn(e, "Exception while stopping task [%s]", id);
      return false;
    }
  }

  @Nullable
  public DateTime getStartTime(final String id)
  {
    log.info("GetStartTime task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(id, HttpMethod.GET, "time/start", null, false);
      return response.getContent() == null || response.getContent().isEmpty()
             ? null
             : deserialize(response.getContent(), DateTime.class);
    }
    catch (NoTaskLocationException | ConnectException e) {
      return null;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public boolean finalize(final String id)
  {
    log.debug("Finalize task[%s]", id);

    try {
      final StringFullResponseHolder response = submitRequestWithEmptyContent(
          id,
          HttpMethod.POST,
          "finalize",
          null,
          false
      );
      return isSuccess(response);
    }
    catch (NoTaskLocationException | IOException e) {
      log.warn(e, "Exception while finalizing task [%s]", id);
      return false;
    }
  }


}
