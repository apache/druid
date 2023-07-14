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

package org.apache.druid.storage.google.output;

import com.google.api.services.storage.model.StorageObject;
import com.google.common.base.Joiner;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.storage.google.GoogleInputDataConfig;
import org.apache.druid.storage.google.GoogleStorage;
import org.apache.druid.storage.google.GoogleUtils;
import org.apache.druid.storage.remote.ChunkingStorageConnector;
import org.apache.druid.storage.remote.ChunkingStorageConnectorParameters;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

public class GoogleStorageConnector extends ChunkingStorageConnector<StorageObject>
{

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();
  private static final Logger log = new Logger(GoogleStorageConnector.class);

  private final GoogleStorage storage;
  private final GoogleOutputConfig config;
  private final GoogleInputDataConfig inputDataConfig;

  public GoogleStorageConnector(
      GoogleStorage storage,
      GoogleOutputConfig config,
      GoogleInputDataConfig inputDataConfig
  )
  {
    this.storage = storage;
    this.config = config;
    this.inputDataConfig = inputDataConfig;
  }


  @Override
  public boolean pathExists(String path)
  {
    return storage.exists(config.getBucket(), objectPath(path));
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    return null;
  }

  @Override
  public void deleteFile(String path) throws IOException
  {
    try {
      final String fullPath = objectPath(path);
      log.debug("Deleting file at bucket: [%s], path: [%s]", config.getBucket(), fullPath);

      GoogleUtils.retryGoogleCloudStorageOperation(
          () -> {
            storage.delete(config.getBucket(), fullPath);
            return null;
          }
      );
    }
    catch (Exception e) {
      log.error("Error occurred while deleting file at path [%s]. Error: [%s]", path, e.getMessage());
      throw new IOException(e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {
    int currentItemSize = 0;


    for (String path : paths) {


    }
  }

  @Override
  public void deleteRecursively(String path) throws IOException
  {

  }

  @Override
  public Iterator<String> listDir(String dirName) throws IOException
  {
    return null;
  }

  @Override
  public ChunkingStorageConnectorParameters<StorageObject> buildInputParams(String path)
  {
    return null;
  }

  @Override
  public ChunkingStorageConnectorParameters<StorageObject> buildInputParams(String path, long from, long size)
  {
    return null;
  }

  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }
}
