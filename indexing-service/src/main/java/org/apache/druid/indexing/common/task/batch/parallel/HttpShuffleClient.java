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

import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Inject;
import io.netty.handler.codec.http.HttpMethod;
import org.apache.druid.guice.annotations.EscalatedClient;
import org.apache.druid.java.util.common.FileUtils;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.Request;
import org.apache.druid.java.util.http.client.response.InputStreamResponseHandler;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.ExecutionException;

/**
 * HTTP-based ShuffleClient.
 * This class is injected as a lazy singleton instance and thus must be stateless.
 */
public class HttpShuffleClient implements ShuffleClient<GenericPartitionLocation>
{
  private static final Logger LOG = new Logger(HttpShuffleClient.class);

  @VisibleForTesting
  static final int NUM_FETCH_RETRIES = 3;

  private static final int BUFFER_SIZE = 1024 * 4;

  private final HttpClient httpClient;

  @Inject
  public HttpShuffleClient(@EscalatedClient HttpClient httpClient)
  {
    this.httpClient = httpClient;
  }

  @Override
  public File fetchSegmentFile(
      File partitionDir,
      String supervisorTaskId,
      GenericPartitionLocation location
  ) throws IOException
  {
    // Create a local buffer since this class is not thread-safe.
    // Note that this method can be called by different threads at the same time with ThreadingTaskRunner.
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
            throw new IOException(e);
          }
        },
        zippedFile,
        buffer,
        t -> t instanceof IOException,
        NUM_FETCH_RETRIES,
        StringUtils.format("Failed to fetch file[%s]", uri)
    );
    final File unzippedDir = new File(partitionDir, StringUtils.format("unzipped_%s", location.getSubTaskId()));
    try {
      FileUtils.mkdirp(unzippedDir);
      CompressionUtils.unzip(zippedFile, unzippedDir);
    }
    finally {
      if (!zippedFile.delete()) {
        LOG.warn("Failed to delete temp file[%s]", zippedFile);
      }
    }
    return unzippedDir;
  }
}
