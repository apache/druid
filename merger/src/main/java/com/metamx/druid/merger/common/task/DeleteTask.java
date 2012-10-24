package com.metamx.druid.merger.common.task;

import com.google.common.collect.Lists;
import com.metamx.common.logger.Logger;
import com.metamx.druid.QueryGranularity;
import com.metamx.druid.aggregation.AggregatorFactory;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.index.v1.IncrementalIndex;
import com.metamx.druid.index.v1.IncrementalIndexAdapter;
import com.metamx.druid.index.v1.IndexMerger;
import com.metamx.druid.index.v1.IndexableAdapter;
import com.metamx.druid.jackson.DefaultObjectMapper;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.shard.NoneShardSpec;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.codehaus.jackson.map.ObjectMapper;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import java.io.File;
import java.util.ArrayList;

public class DeleteTask extends AbstractTask
{
  private static final Logger log = new Logger(DeleteTask.class);

  @JsonCreator
  public DeleteTask(
      @JsonProperty("dataSource") String dataSource,
      @JsonProperty("interval") Interval interval
  )
  {
    super(
        String.format(
            "delete_%s_%s_%s_%s",
            dataSource,
            interval.getStart(),
            interval.getEnd(),
            new DateTime().toString()
        ),
        dataSource,
        interval
    );
  }

  @Override
  public Type getType()
  {
    return Task.Type.DELETE;
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
  {
    // Strategy: Create an empty segment covering the interval to be deleted
    final IncrementalIndex empty = new IncrementalIndex(0, QueryGranularity.NONE, new AggregatorFactory[0]);
    final IndexableAdapter emptyAdapter = new IncrementalIndexAdapter(
        this.getInterval(),
        empty,
        new ArrayList<String>(),
        new ArrayList<String>()
    );

    // Create DataSegment
    final DataSegment segment =
        DataSegment.builder()
                   .dataSource(this.getDataSource())
                   .interval(this.getInterval())
                   .version(context.getVersion())
                   .shardSpec(new NoneShardSpec())
                   .build();

    final File outDir = new File(toolbox.getConfig().getTaskDir(this), segment.getIdentifier());
    final File fileToUpload = IndexMerger.merge(Lists.newArrayList(emptyAdapter), new AggregatorFactory[0], outDir);

    // Upload the segment
    final DataSegment uploadedSegment = toolbox.getSegmentPusher().push(fileToUpload, segment);

    log.info(
        "Uploaded tombstone segment for[%s] interval[%s] with version[%s]",
        segment.getDataSource(),
        segment.getInterval(),
        segment.getVersion()
    );

    return TaskStatus.success(getId(), Lists.newArrayList(uploadedSegment));
  }
}
