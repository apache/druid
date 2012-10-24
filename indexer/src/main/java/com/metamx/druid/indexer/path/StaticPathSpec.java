package com.metamx.druid.indexer.path;

import com.metamx.common.logger.Logger;
import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.codehaus.jackson.annotate.JsonProperty;

import java.io.IOException;

/**
 * Class uses public fields to work around http://jira.codehaus.org/browse/MSHADE-92
 *
 * Adjust to JsonCreator and final fields when resolved.
 */
public class StaticPathSpec implements PathSpec
{
  private static final Logger log = new Logger(StaticPathSpec.class);

  @JsonProperty("paths")
  public String paths;

  public StaticPathSpec()
  {
    this(null);
  }

  public StaticPathSpec(
      String paths
  )
  {
    this.paths = paths;
  }

  @Override
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException
  {
    log.info("Adding paths[%s]", paths);
    FileInputFormat.addInputPaths(job, paths);
    return job;
  }
}
