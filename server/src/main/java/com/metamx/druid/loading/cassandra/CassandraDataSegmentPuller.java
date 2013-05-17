/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
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

package com.metamx.druid.loading.cassandra;

import java.io.File;
import java.io.OutputStream;

import org.apache.commons.io.FileUtils;

import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.utils.CompressionUtils;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;

/**
 * Cassandra Segment Puller
 *
 * @author boneill42
 */
public class CassandraDataSegmentPuller extends CassandraStorage implements DataSegmentPuller
{
	private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
	private static final int CONCURRENCY = 10;
	private static final int BATCH_SIZE = 10;

	public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
	{
		super(config);
	}

	@Override
	public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException
	{
		String key = (String) segment.getLoadSpec().get("key");
		log.info("Pulling index from C* at path[%s] to outDir[%s]", key, outDir);

		if (!outDir.exists())
		{
			outDir.mkdirs();
		}

		if (!outDir.isDirectory())
		{
			throw new ISE("outDir[%s] must be a directory.", outDir);
		}

		long startTime = System.currentTimeMillis();
		ObjectMetadata meta = null;
		final File outFile = new File(outDir, "index.zip");
		try
		{
			try
			{
				log.info("Writing to [%s]", outFile.getAbsolutePath());
				OutputStream os = Files.newOutputStreamSupplier(outFile).getOutput();
				meta = ChunkedStorage
				    .newReader(indexStorage, key, os)
				    .withBatchSize(BATCH_SIZE)
				    .withConcurrencyLevel(CONCURRENCY)
				    .call();
				os.close();
				CompressionUtils.unzip(outFile, outDir);
			} catch (Exception e)
			{
				FileUtils.deleteDirectory(outDir);
			}
		} catch (Exception e)
		{
			throw new SegmentLoadingException(e, e.getMessage());
		}
		log.info("Pull of file[%s] completed in %,d millis (%s bytes)", key, System.currentTimeMillis() - startTime,
		    meta.getObjectSize());
	}

	@Override
	public long getLastModified(DataSegment segment) throws SegmentLoadingException
	{
		String key = (String) segment.getLoadSpec().get("key");
		OperationResult<ColumnList<String>> result;
		try
		{
			result = this.keyspace.prepareQuery(descriptorStorage)
			    .getKey(key)
			    .execute();
			ColumnList<String> children = result.getResult();
			long lastModified = children.getColumnByName("lastmodified").getLongValue();
			log.info("Read lastModified for [%s] as [%d]", key, lastModified);
			return lastModified;
		} catch (ConnectionException e)
		{
			throw new SegmentLoadingException(e, e.getMessage());
		}
	}
}
