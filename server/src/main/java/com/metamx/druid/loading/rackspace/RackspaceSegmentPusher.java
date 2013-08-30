package com.metamx.druid.loading.rackspace;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileExistsException;
import org.apache.http.HttpException;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IndexIO;
import com.metamx.druid.loading.DataSegmentPusher;
import com.metamx.druid.realtime.RealtimeNode;
import com.metamx.druid.utils.CompressionUtils;
import com.metamx.emitter.EmittingLogger;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesException;
import com.rackspacecloud.client.cloudfiles.FilesObject;

/**
 * Pushes data segments to blob-store. Two different files are pushed : 
 * <ul><li> an index file in zip format</li><li> a descriptor file as json</ul></li>
 * @author Nitin Mane
 *
 */
public class RackspaceSegmentPusher implements DataSegmentPusher{

	private static final EmittingLogger log = new EmittingLogger(RackspaceSegmentPusher.class);
	private static final Joiner JOINER = Joiner.on("/").skipNulls();
	
	private final FilesClient 			rsClient ;
	private final RackspaceSegmentPusherConfig 	rsPusherConfig ;
	private final ObjectMapper 			jsonMapper ;
		
	public RackspaceSegmentPusher(FilesClient rsClient, RackspaceSegmentPusherConfig rsPusherConfig, ObjectMapper jsonMapper) 
	{
		this.rsClient = rsClient;
		this.rsPusherConfig = rsPusherConfig;
		this.jsonMapper = jsonMapper;
	}
	
	public DataSegment push(File file, DataSegment segment) throws IOException 
	{			
		
		long 			indexSize 		= 0;
		
		String 			bucketName 		= null;
		String 			fileName 		= null;		
		
		File 			descriptorFile	= null;
		
		
		log.info("Uploading [%s] to Rackspace.", file);		
		
		fileName = JOINER.join(segment.getDataSource(), 
								String.format("%s_%s",segment.getInterval().getStart(),segment.getInterval().getEnd()), 
								segment.getVersion(), 
								segment.getShardSpec().getPartitionNum());
		
	    	bucketName = this.rsPusherConfig.getFileContainer();
	    
	    	File indexFilesDir = file;
	    
	    	final File zipOutFile = File.createTempFile("admaxim", AppConstant.INDEX_FILE.getValue());   
		indexSize = CompressionUtils.zip(indexFilesDir, zipOutFile);    
	      
    		try {
			log.info("Pushing Index File %s.",zipOutFile.getName());
			
			if(pushFileToRackspace(zipOutFile, fileName+"/"+AppConstant.INDEX_FILE.getValue()) != null) {
				log.info("SUCCESS!! : Pushed %s Index File.",fileName+"/"+AppConstant.INDEX_FILE.getValue()+" ["+zipOutFile.getName()+"]");
			}
			segment = segment.withSize(indexSize).withLoadSpec(ImmutableMap.<String, Object>of
																					("type","rs_zip",
																							AppConstant.FILE_CONTAINER.getValue(),bucketName,
																							AppConstant.FILE_PATH.getValue(),fileName+"/"+AppConstant.INDEX_FILE.getValue())
																					).withBinaryVersion(IndexIO.getVersionFromDir(indexFilesDir));
			
			descriptorFile = File.createTempFile("admaxim", AppConstant.DESCRIPTOR_FILE.getValue());
			//StreamUtils.copyToFileAndClose(new ByteArrayInputStream(this.jsonMapper.writeValueAsBytes(segment)), descriptorFile);
			Files.copy(ByteStreams.newInputStreamSupplier(this.jsonMapper.writeValueAsBytes(segment)), descriptorFile);
			
			log.info("Pushing Descriptor File %s.",descriptorFile.getName());
			
			if(pushFileToRackspace(descriptorFile, fileName+"/"+AppConstant.DESCRIPTOR_FILE.getValue()) != null) {
				log.info("SUCCESS!! : Pushed %s Descriptor File.",fileName+"/"+AppConstant.DESCRIPTOR_FILE.getValue()+" ["+descriptorFile.getName()+"]");	
			}			
			
		} catch (FilesException e) {			
			segment = null;
			log.error("RackspaceSegmentPusher.push() : %s", "FilesException: "+e.getMessage());
			e.printStackTrace();
		} catch (HttpException e) {
			segment = null;
			log.error("RackspaceSegmentPusher.push() : %s", "HttpException: "+e.getMessage());
			e.printStackTrace();
		}    	
	    	finally
	    	{
	    		log.info("Deleting Zipped Index File [%s]", zipOutFile);
			zipOutFile.delete();

			log.info("Deleting Descriptor file [%s]", descriptorFile);
			descriptorFile.delete();
	    	}
    	
	   return segment;
	}
	private String pushFileToRackspace(File zipOutFile, String flName) throws FilesException, IOException, HttpException 
	{		
		return this.rsClient.storeObjectAs(this.rsPusherConfig.getFileContainer(), zipOutFile, "application/octet-stream", flName);
	}

}
