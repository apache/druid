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

package org.apache.druid.storage.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.segment.SegmentUtils;
import org.apache.druid.segment.loading.DataSegmentPusher;
import org.apache.druid.timeline.DataSegment;
import org.apache.druid.utils.CompressionUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.util.Map;

/**
 * Cassandra Segment Pusher
 */
public class CassandraDataSegmentPusher extends CassandraStorage implements DataSegmentPusher
{
  private static final Logger log = new Logger(CassandraDataSegmentPusher.class);
  private static final int CONCURRENCY = 10;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
  private final ObjectMapper jsonMapper;

  @Inject
  public CassandraDataSegmentPusher(
      CassandraDataSegmentConfig config,
      ObjectMapper jsonMapper
  )
  {
    super(config);
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String getPathForHadoop()
  {
    throw new UnsupportedOperationException("Cassandra storage does not support indexing via Hadoop");
  }

  @Deprecated
  @Override
  public String getPathForHadoop(String dataSource)
  {
    return getPathForHadoop();
  }

  @Override
  public DataSegment push(final File indexFilesDir, DataSegment segment, final boolean useUniquePath) throws IOException
  {
    log.info("Writing [%s] to C*", indexFilesDir);
    String key = JOINER.join(
        config.getKeyspace().isEmpty() ? null : config.getKeyspace(),
        this.getStorageDir(segment, useUniquePath)
    );

    // Create index
    final File compressedIndexFile = File.createTempFile("druid", "index.zip");
    long indexSize = CompressionUtils.zip(indexFilesDir, compressedIndexFile);
    log.info("Wrote compressed file [%s] to [%s]", compressedIndexFile.getAbsolutePath(), key);

    int version = SegmentUtils.getVersionFromDir(indexFilesDir);

    try (final InputStream fileStream = Files.newInputStream(compressedIndexFile.toPath())) {
      long start = System.currentTimeMillis();
      ChunkedStorage.newWriter(indexStorage, key, fileStream)
                    .withConcurrencyLevel(CONCURRENCY).call();
      byte[] json = jsonMapper.writeValueAsBytes(segment);
      MutationBatch mutation = this.keyspace.prepareMutationBatch();
      mutation.withRow(descriptorStorage, key)
              .putColumn("lastmodified", System.currentTimeMillis(), null)
              .putColumn("descriptor", json, null);
      mutation.execute();
      log.info("Wrote index to C* in [%s] ms", System.currentTimeMillis() - start);
    }
    catch (Exception e) {
      throw new IOException(e);
    }

    segment = segment.withSize(indexSize)
                     .withLoadSpec(ImmutableMap.of("type", "c*", "key", key))
                     .withBinaryVersion(version);

    log.info("Deleting zipped index File[%s]", compressedIndexFile);
    compressedIndexFile.delete();
    return segment;
  }

  @Override
  public Map<String, Object> makeLoadSpec(URI uri)
  {
    throw new UnsupportedOperationException("not supported");
  }

  private boolean doesObjectExist(ChunkedStorageProvider provider, String objectName) throws Exception
  {
    try {
      return ChunkedStorage.newInfoReader(provider, objectName).call().isValidForRead();
    }
    catch (NotFoundException e) {
      return false;
    }
  }
}
