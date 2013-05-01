package com.metamx.druid.loading;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.utils.CompressionUtils;

/**
 */
public class CassandraDataSegmentPusher implements DataSegmentPusher
{
	private static final Logger log = new Logger(CassandraDataSegmentPusher.class);

	private final CassandraDataSegmentPusherConfig config;
  private static final Joiner JOINER = Joiner.on("/").skipNulls();
	private final ObjectMapper jsonMapper;
	private Cluster cluster;
	private Session session;
	private String keyspace = null;
	private String table = null;
	

	public CassandraDataSegmentPusher(
	    CassandraDataSegmentPusherConfig config,
	    ObjectMapper jsonMapper)
	{
		this.config = config;
		this.jsonMapper = jsonMapper;
		this.keyspace = this.config.getKeyspace();
		this.table = this.config.getTable();
		
		cluster = Cluster.builder().addContactPoints(this.config.getHost()).build();
		session = cluster.connect();
		session.execute("USE " + keyspace);
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
    
    String statement = "INSERT INTO " + keyspace + "." + table + "(key, version, descriptor, index) VALUES (?,?,?,?)";
    PreparedStatement ps = session.prepare(statement);
    byte[] indexData = ByteStreams.toByteArray(new FileInputStream(compressedIndexFile));    
    byte[] descriptorData = ByteStreams.toByteArray(new FileInputStream(compressedIndexFile));    
    BoundStatement bs = ps.bind(key, version, descriptorData, indexData);
    session.execute(bs);
    
    segment = segment.withSize(indexSize)
        .withLoadSpec(
            ImmutableMap.<String, Object>of("type", "c*", "key", key)
        )
        .withBinaryVersion(IndexIO.getVersionFromDir(indexFilesDir));

    
    log.info("Deleting zipped index File[%s]", compressedIndexFile);
    compressedIndexFile.delete();
    log.info("Deleting descriptor file[%s]", descriptorFile);
    descriptorFile.delete();
    return segment;
	}
}
