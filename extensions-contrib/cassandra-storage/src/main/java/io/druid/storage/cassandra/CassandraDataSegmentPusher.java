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

package io.druid.storage.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;

import io.druid.java.util.common.CompressionUtils;
import io.druid.java.util.common.logger.Logger;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

/**
 * Cassandra Segment Pusher
 *
 * @author boneill42
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
	    ObjectMapper jsonMapper)
	{
		super(config);
		this.jsonMapper=jsonMapper;
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
  public DataSegment push(final File indexFilesDir, DataSegment segment) throws IOException
  {
    log.info("Writing [%s] to C*", indexFilesDir);
    String key = JOINER.join(
        config.getKeyspace().isEmpty() ? null : config.getKeyspace(),
		    DataSegmentPusherUtil.getStorageDir(segment)
		    );

		// Create index
		final File compressedIndexFile = File.createTempFile("druid", "index.zip");
		long indexSize = CompressionUtils.zip(indexFilesDir, compressedIndexFile);
		log.info("Wrote compressed file [%s] to [%s]", compressedIndexFile.getAbsolutePath(), key);

		int version = SegmentUtils.getVersionFromDir(indexFilesDir);

		try
		{
			long start = System.currentTimeMillis();
			ChunkedStorage.newWriter(indexStorage, key, new FileInputStream(compressedIndexFile))
			    .withConcurrencyLevel(CONCURRENCY).call();
			byte[] json = jsonMapper.writeValueAsBytes(segment);
			MutationBatch mutation = this.keyspace.prepareMutationBatch();
      mutation.withRow(descriptorStorage, key)
      	.putColumn("lastmodified", System.currentTimeMillis(), null)
      	.putColumn("descriptor", json, null);      	
      mutation.execute();
			log.info("Wrote index to C* in [%s] ms", System.currentTimeMillis() - start);
		} catch (Exception e)
		{
			throw new IOException(e);
		}

		segment = segment.withSize(indexSize)
		    .withLoadSpec(
		        ImmutableMap.<String, Object> of("type", "c*", "key", key)
		    )
		    .withBinaryVersion(version);

		log.info("Deleting zipped index File[%s]", compressedIndexFile);
		compressedIndexFile.delete();
		return segment;
	}
}
