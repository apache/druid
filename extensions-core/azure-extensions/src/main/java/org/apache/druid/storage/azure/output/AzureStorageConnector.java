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

package org.apache.druid.storage.azure.output;

import com.azure.storage.blob.batch.BlobBatchStorageException;
import com.azure.storage.blob.models.BlobStorageException;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;
import org.apache.druid.data.input.impl.prefetch.ObjectOpenFunction;
import org.apache.druid.storage.azure.AzureStorage;
import org.apache.druid.storage.azure.AzureUtils;
import org.apache.druid.storage.remote.ChunkingStorageConnector;
import org.apache.druid.storage.remote.ChunkingStorageConnectorParameters;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Implementation of the storage connector that facilitates reading and writing from Azure's blob storage.
 * This extends the {@link ChunkingStorageConnector} so that the downloads are in a chunked manner.
 */
public class AzureStorageConnector extends ChunkingStorageConnector<AzureInputRange>
{

  private static final String DELIM = "/";
  private static final Joiner JOINER = Joiner.on(DELIM).skipNulls();

  private final AzureOutputConfig config;
  private final AzureStorage azureStorage;

  public AzureStorageConnector(
      final AzureOutputConfig config,
      final AzureStorage azureStorage
  )
  {
    this.config = config;
    this.azureStorage = azureStorage;
  }

  @Override
  public ChunkingStorageConnectorParameters<AzureInputRange> buildInputParams(String path) throws IOException
  {
    try {
      return buildInputParams(path, 0, azureStorage.getBlockBlobLength(config.getContainer(), objectPath(path)));
    }
    catch (BlobStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public ChunkingStorageConnectorParameters<AzureInputRange> buildInputParams(String path, long from, long size)
  {
    ChunkingStorageConnectorParameters.Builder<AzureInputRange> parameters = new ChunkingStorageConnectorParameters.Builder<>();
    parameters.tempDirSupplier(config::getTempDir);
    parameters.maxRetry(config.getMaxRetry());
    parameters.cloudStoragePath(objectPath(path));
    parameters.retryCondition(AzureUtils.AZURE_RETRY);
    parameters.start(from);
    parameters.end(from + size);
    parameters.objectSupplier((start, end) -> new AzureInputRange(
        start,
        end - start,
        config.getContainer(),
        objectPath(path)
    ));
    parameters.objectOpenFunction(
        new ObjectOpenFunction<AzureInputRange>()
        {
          @Override
          public InputStream open(AzureInputRange inputRange) throws IOException
          {
            try {
              return azureStorage.getBlockBlobInputStream(
                  inputRange.getStart(),
                  inputRange.getSize(),
                  inputRange.getContainer(),
                  inputRange.getPath(),
                  config.getMaxRetry()
              );
            }
            catch (BlobStorageException e) {
              throw new IOException(e);
            }
          }

          @Override
          public InputStream open(AzureInputRange inputRange, long offset) throws IOException
          {
            AzureInputRange newInputRange = new AzureInputRange(
                inputRange.getStart() + offset,
                Math.max(inputRange.getSize() - offset, 0),
                inputRange.getContainer(),
                inputRange.getPath()
            );
            return open(newInputRange);
          }
        }
    );

    return parameters.build();
  }

  @Override
  public boolean pathExists(String path) throws IOException
  {
    try {
      return azureStorage.getBlockBlobExists(config.getContainer(), objectPath(path), config.getMaxRetry());
    }
    catch (BlobStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public OutputStream write(String path) throws IOException
  {
    try {
      return azureStorage.getBlockBlobOutputStream(
          config.getContainer(),
          objectPath(path),
          config.getChunkSize().getBytesInInt(),
          config.getMaxRetry()
      );
    }
    catch (BlobStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteFile(String path) throws IOException
  {
    try {
      azureStorage.batchDeleteFiles(
          config.getContainer(),
          Collections.singletonList(objectPath(path)),
          config.getMaxRetry()
      );
    }
    catch (BlobStorageException | BlobBatchStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteFiles(Iterable<String> paths) throws IOException
  {
    try {
      azureStorage.batchDeleteFiles(
          config.getContainer(),
          Iterables.transform(paths, this::objectPath),
          config.getMaxRetry()
      );
    }
    catch (BlobStorageException | BlobBatchStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteRecursively(String path) throws IOException
  {
    try {
      azureStorage.emptyCloudBlobDirectory(config.getContainer(), objectPath(path), config.getMaxRetry());
    }
    catch (BlobStorageException e) {
      throw new IOException(e);
    }
  }

  @Override
  public Iterator<String> listDir(String dirName) throws IOException
  {
    final String prefixBasePath = objectPath(dirName);
    List<String> paths;
    try {
      paths = azureStorage.listDir(config.getContainer(), prefixBasePath, config.getMaxRetry());
    }
    catch (BlobStorageException e) {
      throw new IOException(e);
    }

    return paths.stream().map(path -> {
      String[] size = path.split(prefixBasePath, 2);
      if (size.length > 1) {
        return size[1];
      } else {
        return "";
      }
    }).iterator();
  }

  private String objectPath(String path)
  {
    return JOINER.join(config.getPrefix(), path);
  }
}
