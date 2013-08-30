package com.metamx.druid.loading.rackspace;

import java.io.File;

import org.skife.config.Config;

public abstract class RackspaceSegmentPullerConfig {
	
	@Config("druid.paths.indexCache")
	abstract public File getCacheDirectory(); 

}
