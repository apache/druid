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
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.common.io.Files;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 */
public class CassandraDataSegmentPuller implements DataSegmentPuller
{
	private static final Logger log = new Logger(CassandraDataSegmentPuller.class);
	private static final String CLUSTER_NAME = "druid_cassandra_cluster";
	private static final String INDEX_TABLE_NAME = "index_storage";
	private static final String DESCRIPTOR_TABLE_NAME = "descriptor_storage";
	private static final int CONCURRENCY = 10;
	private static final int BATCH_SIZE = 10;

	private Keyspace keyspace;
	private AstyanaxContext<Keyspace> astyanaxContext;
	private ChunkedStorageProvider indexStorage;
	private ColumnFamily<String, String> descriptorStorage;

	public CassandraDataSegmentPuller(CassandraDataSegmentConfig config)
	{
		this.astyanaxContext = new AstyanaxContext.Builder()
		    .forCluster(CLUSTER_NAME)
		    .forKeyspace(config.getKeyspace())
		    .withAstyanaxConfiguration(new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.NONE))
		    .withConnectionPoolConfiguration(
		        new ConnectionPoolConfigurationImpl("MyConnectionPool").setMaxConnsPerHost(10)
		            .setSeeds(config.getHost())).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
		    .buildKeyspace(ThriftFamilyFactory.getInstance());
		this.astyanaxContext.start();
		this.keyspace = this.astyanaxContext.getEntity();

		indexStorage = new CassandraChunkedStorageProvider(keyspace, INDEX_TABLE_NAME);

		descriptorStorage = new ColumnFamily<String, String>(DESCRIPTOR_TABLE_NAME,
		    StringSerializer.get(), StringSerializer.get());
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
		try
		{
			final File outFile = new File(outDir, toFilename(key, ".gz"));
			meta = ChunkedStorage
			    .newReader(indexStorage, key, Files.newOutputStreamSupplier(outFile).getOutput())
			    .withBatchSize(BATCH_SIZE)
			    .withConcurrencyLevel(CONCURRENCY)
			    .call();
		} catch (Exception e)
		{
			log.error("Could not pull segment [" + key + "] from C*", e);
			try
			{
				FileUtils.deleteDirectory(outDir);
			} catch (IOException ioe)
			{
				log.error("Couldn't delete directory [" + outDir + "] for cleanup.", ioe);
			}
		}

		log.info("Pull of file[%s] completed in %,d millis (%s chunks)", key, System.currentTimeMillis() - startTime,
		    meta.getChunkSize());

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
			log.info("Read lastModified for [" + key + "] as [" + lastModified + "]");
			return lastModified;
		} catch (ConnectionException e)
		{
			throw new SegmentLoadingException(e, e.getMessage());
		}
	}

	private String toFilename(String key, final String suffix)
	{
		String filename = key.substring(key.lastIndexOf("/") + 1);
		filename = filename.substring(0, filename.length() - suffix.length());
		return filename;
	}
}
