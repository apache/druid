package com.metamx.druid.loading;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.utils.CompressionUtils;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ObjectMetadata;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * This is the data segment pusher for Cassandra.
 * 
 * Create the schema in cli with: 
 * CREATE COLUMN FAMILY indexStorage WITH comparator = UTF8Type AND key_validation_class=UTF8Type 
 * CREATE COLUMN FAMILY descriptorStorage WITH comparator = UTF8Type AND key_validation_class=UTF8Type
 */
// TODO: Auto-create the schema if it does not exist.
// Should we make it so they can specify tables?
public class CassandraDataSegmentPusher implements DataSegmentPusher
{
	private static final Logger log = new Logger(CassandraDataSegmentPusher.class);
	private static final String CLUSTER_NAME = "druid_cassandra_cluster";
	private static final String INDEX_TABLE_NAME = "index_storage";
	private static final String DESCRIPTOR_TABLE_NAME = "descriptor_storage";
	private static final int CONCURRENCY = 10;
	private static final Joiner JOINER = Joiner.on("/").skipNulls();

	private final CassandraDataSegmentPusherConfig config;
	private final ObjectMapper jsonMapper;

	private Keyspace keyspace;
	private AstyanaxContext<Keyspace> astyanaxContext;
	private ChunkedStorageProvider indexStorage;
	private ChunkedStorageProvider descriptorStorage;

	public CassandraDataSegmentPusher(
	    CassandraDataSegmentPusherConfig config,
	    ObjectMapper jsonMapper)
	{
		this.config = config;
		this.jsonMapper = jsonMapper;
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
			descriptorStorage = new CassandraChunkedStorageProvider(keyspace, DESCRIPTOR_TABLE_NAME);
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
		int version = IndexIO.getVersionFromDir(indexFilesDir);

		// Create descriptor
		File descriptorFile = File.createTempFile("druid", "descriptor.json");
		Files.copy(ByteStreams.newInputStreamSupplier(jsonMapper.writeValueAsBytes(segment)), descriptorFile);

		try
		{
			ObjectMetadata indexMeta = ChunkedStorage.newWriter(indexStorage, key, new FileInputStream(compressedIndexFile))
			    .withConcurrencyLevel(CONCURRENCY).call();

			ObjectMetadata descriptorMeta = ChunkedStorage
			    .newWriter(descriptorStorage, key, new FileInputStream(descriptorFile))
			    .withConcurrencyLevel(CONCURRENCY).call();

			log.debug("Wrote index to C* [" + indexMeta.getParentPath() + "]");
			log.debug("Wrote descriptor to C* [" + descriptorMeta.getParentPath() + "]");
		} catch (Exception e)
		{
			throw new IOException(e);
		}

		segment = segment.withSize(indexSize)
		    .withLoadSpec(
		        ImmutableMap.<String, Object> of("type", "c*", "key", key)
		    )
		    .withBinaryVersion(IndexIO.getVersionFromDir(indexFilesDir));

		log.info("Deleting zipped index File[%s]", compressedIndexFile);
		compressedIndexFile.delete();
		log.info("Deleting descriptor file[%s]", descriptorFile);
		descriptorFile.delete();
		return segment;
	}
}
