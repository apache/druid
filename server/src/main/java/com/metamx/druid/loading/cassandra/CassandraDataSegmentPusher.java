package com.metamx.druid.loading.cassandra;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.loading.DataSegmentPusherUtil;
import com.metamx.druid.utils.CompressionUtils;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.recipes.storage.CassandraChunkedStorageProvider;
import com.netflix.astyanax.recipes.storage.ChunkedStorage;
import com.netflix.astyanax.recipes.storage.ChunkedStorageProvider;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;

/**
 * Cassandra Segment Pusher
 *
 * @author boneill42
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
	private final CassandraDataSegmentConfig config;
	private final ObjectMapper jsonMapper;

	private Keyspace keyspace;
	private AstyanaxContext<Keyspace> astyanaxContext;
	private ChunkedStorageProvider indexStorage;
	private ColumnFamily<String, String> descriptorStorage;

	public CassandraDataSegmentPusher(
	    CassandraDataSegmentConfig config,
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
	    
			descriptorStorage = new ColumnFamily<String, String>(DESCRIPTOR_TABLE_NAME, 
					StringSerializer.get(), StringSerializer.get());
			
			indexStorage = new CassandraChunkedStorageProvider(keyspace, INDEX_TABLE_NAME);
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

		int version = IndexIO.getVersionFromDir(indexFilesDir);

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
