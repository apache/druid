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

package org.apache.druid.indexing.common.task.batch.parallel;

import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

public class HttpShuffleClient implements ShuffleClient
{
  private static final int BUFFER_SIZE = 1024 * 4;
  private static final int NUM_FETCH_RETRIES = 3;

  private final HttpClient httpClient;

  @Inject
  public HttpShuffleClient(@EscalatedClient HttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  @Override
  public <T, P extends PartitionLocation<T>> File fetchSegmentFile(
      File partitionDir,
      String supervisorTaskId,
      P location
  ) throws IOException
  {
    final byte[] buffer = new byte[BUFFER_SIZE];
    final File zippedFile = new File(partitionDir, StringUtils.format("temp_%s", location.getSubTaskId()));
    final URI uri = location.toIntermediaryDataServerURI(supervisorTaskId);
    FileUtils.copyLarge(
        uri,
        u -> {
          try {
            return httpClient.go(new Request(HttpMethod.GET, u.toURL()), new InputStreamResponseHandler())
                             .get();
          }
          catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
          }
        },
        zippedFile,
        buffer,
        t -> t instanceof IOException,
        NUM_FETCH_RETRIES,
        StringUtils.format("Failed to fetch file[%s]", uri)
    );
    return zippedFile;
  }
}
