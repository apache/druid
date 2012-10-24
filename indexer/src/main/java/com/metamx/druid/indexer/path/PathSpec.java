package com.metamx.druid.indexer.path;

import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.hadoop.mapreduce.Job;
import org.codehaus.jackson.annotate.JsonSubTypes;
import org.codehaus.jackson.annotate.JsonTypeInfo;

import java.io.IOException;

/**
 */
@JsonTypeInfo(use=JsonTypeInfo.Id.NAME, property="type")
@JsonSubTypes(value={
    @JsonSubTypes.Type(name="granular_unprocessed", value=GranularUnprocessedPathSpec.class),
    @JsonSubTypes.Type(name="granularity", value=GranularityPathSpec.class),
    @JsonSubTypes.Type(name="static", value=StaticPathSpec.class)
})
public interface PathSpec
{
  public Job addInputPaths(HadoopDruidIndexerConfig config, Job job) throws IOException;
}
