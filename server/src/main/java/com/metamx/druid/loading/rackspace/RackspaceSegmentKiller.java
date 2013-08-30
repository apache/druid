package com.metamx.druid.loading.rackspace;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpException;

import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.loading.DataSegmentKiller;
import com.metamx.druid.loading.S3DataSegmentKiller;
import com.metamx.druid.loading.SegmentLoadingException;
import com.rackspacecloud.client.cloudfiles.FilesClient;

public class RackspaceSegmentKiller implements DataSegmentKiller {

	private static final Logger log = new Logger(RackspaceSegmentKiller.class);
	
	final FilesClient rsClient ;
	
	public RackspaceSegmentKiller(FilesClient rsClient) {
		this.rsClient = rsClient;
	}
	
	public void kill(DataSegment segments) throws SegmentLoadingException 
	{
		Map<String, Object> loadSpec = segments.getLoadSpec();
		String fileContainer = (String) loadSpec.get(AppConstant.FILE_CONTAINER.getValue());
		String filePath = (String) loadSpec.get(AppConstant.FILE_PATH.getValue());
		String descriptorPath = filePath.substring(0, filePath.lastIndexOf("/"))+"/"+AppConstant.DESCRIPTOR_FILE.getValue();

		try 
		{
			if(!this.rsClient.isLoggedin())
				this.rsClient.login();
			
			// Delete index file and descriptor file respectively
			log.info("Deleting index file: [%s]", filePath);
			this.rsClient.deleteObject(fileContainer, filePath);
			
			log.info("Deleting desc. file: [%s]", descriptorPath);
			this.rsClient.deleteObject(fileContainer, descriptorPath);
			
		} catch (IOException e) {		
			e.printStackTrace();
		} catch (HttpException e) {			
			e.printStackTrace();
		} catch (Exception e){
			log.error("Exception: [%s]-[%s]", e.getClass().getCanonicalName(), e.getLocalizedMessage());
		}
	}

}
