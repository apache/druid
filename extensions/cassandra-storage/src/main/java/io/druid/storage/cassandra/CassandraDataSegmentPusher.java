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

package io.druid.storage.cassandra;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import com.metamx.common.logger.Logger;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import io.druid.segment.SegmentUtils;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.loading.DataSegmentPusherUtil;
import io.druid.timeline.DataSegment;
import io.druid.utils.CompressionUtils;

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
  public String getPathForHadoop(String dataSource)
  {
    throw new UnsupportedOperationException("Cassandra storage does not support indexing via Hadoop");
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
