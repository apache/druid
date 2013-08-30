package com.metamx.druid.loading.rackspace;

import org.skife.config.Config;

public abstract class RackspaceSegmentPusherConfig {
	
	  @Config("druid.pusher.rs.bucket")
	  public abstract String getFileContainer(); 

}
