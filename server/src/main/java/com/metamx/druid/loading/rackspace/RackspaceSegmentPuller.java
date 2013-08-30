package com.metamx.druid.loading.rackspace;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpException;

import com.google.common.io.ByteStreams;
import com.google.common.io.Closeables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import com.metamx.common.ISE;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.DataSegmentPuller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.metamx.druid.utils.CompressionUtils;
import com.metamx.emitter.EmittingLogger;
import com.rackspacecloud.client.cloudfiles.FilesAuthorizationException;
import com.rackspacecloud.client.cloudfiles.FilesClient;
import com.rackspacecloud.client.cloudfiles.FilesInvalidNameException;
import com.rackspacecloud.client.cloudfiles.FilesNotFoundException;
import com.rackspacecloud.client.cloudfiles.FilesObjectMetaData;

/**
 * Pulls segment files from blob-store.
 * @author Nitin Mane
 *
 */
public class RackspaceSegmentPuller implements DataSegmentPuller{

	private static EmittingLogger log = new EmittingLogger(RackspaceSegmentPuller.class);
	
	private final FilesClient 					rsClient ;
		
	@Inject
	public RackspaceSegmentPuller(FilesClient rsClient) 
	{
		this.rsClient = rsClient;
	}
	
	public void getSegmentFiles(DataSegment segment, File outDir) throws SegmentLoadingException 
	{
		Map<String, Object> segmentLoadSpec = null; 
		String fileContainer = null;
		String filePath = null;	
		
		segmentLoadSpec = segment.getLoadSpec();		
		fileContainer = MapUtils.getString(segmentLoadSpec, AppConstant.FILE_CONTAINER.getValue());
		filePath = MapUtils.getString(segmentLoadSpec, AppConstant.FILE_PATH.getValue());
		
    	log.info("Pulling index at path[%s] to outDir[%s]", filePath, outDir);	   

    	if (!outDir.exists()) {
	      		outDir.mkdirs();
    	}

    	if (!outDir.isDirectory()) {
      		throw new ISE("outDir[%s] must be a directory.", outDir);
    	}

    	long startTime = System.currentTimeMillis();
    	try {
      		InputStream in = null;
      		try {
        		in = this.rsClient.getObjectAsStream(fileContainer, filePath);
        
        		if (filePath.endsWith(".zip")) {
          			CompressionUtils.unzip(in, outDir);
        		} else if (filePath.endsWith(".gz")) {
          			final File outFile = new File(outDir, toFilename(filePath, ".gz"));
          			ByteStreams.copy(new GZIPInputStream(in), Files.newOutputStreamSupplier(outFile));
        		} else {
          			ByteStreams.copy(in, Files.newOutputStreamSupplier(new File(outDir, toFilename(filePath, ""))));
        		}
        
        		log.info("Pull of file[%s] completed in [%,d] millis", filePath, System.currentTimeMillis() - startTime);
      		}
      		catch (FilesNotFoundException e){
    	  		FileUtils.deleteDirectory(outDir);
    	  		log.error("File [%s] not found", filePath);
    	  		throw new SegmentLoadingException(e, "File [%s] not found", filePath);
      		}
      		catch (IOException e) {
        		FileUtils.deleteDirectory(outDir);
        		throw new SegmentLoadingException(e, "Problem decompressing file[%s]", filePath);
      		}
      		finally {
        		Closeables.closeQuietly(in);
      		}
	    }
	    catch (Exception e) {
	      throw new SegmentLoadingException(e, e.getMessage());
	    }
		
	}

	public long getLastModified(DataSegment segment) throws SegmentLoadingException {
		
		FilesObjectMetaData fileMetadata = null;
		Map<String, Object> loadSpec = segment.getLoadSpec();
		long lastModified = 0L;
		
		try {
			fileMetadata = this.rsClient.getObjectMetaData((String)loadSpec.get(AppConstant.FILE_CONTAINER.getValue()), (String)loadSpec.get(AppConstant.FILE_PATH.getValue()));			
			SimpleDateFormat sdf =  new SimpleDateFormat("EEE, dd MMM yyyy HH:mm:ss z");						
			lastModified = sdf.parse(fileMetadata.getLastModified()).getTime();
			log.debug("File [%s] was Last Modified on [%,d]", loadSpec.get(AppConstant.FILE_PATH.getValue()), lastModified);
			
		} catch (FilesNotFoundException e) {			
			e.printStackTrace();
		} catch (FilesAuthorizationException e) {
			lastModified = 0L;
			e.printStackTrace();
		} catch (FilesInvalidNameException e) {
			lastModified = 0L;
			e.printStackTrace();
		} catch (IOException e) {
			lastModified = 0L;
			e.printStackTrace();
		} catch (HttpException e) {
			lastModified = 0L;
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return lastModified;
	}
}
