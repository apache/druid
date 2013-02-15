package com.metamx.druid.merger.common.task;

import com.google.common.collect.ImmutableList;
import com.metamx.common.logger.Logger;
import com.metamx.druid.client.DataSegment;
import com.metamx.druid.indexer.HadoopDruidIndexerConfig;
import com.metamx.druid.indexer.HadoopDruidIndexerJob;
import com.metamx.druid.loading.S3SegmentPusher;
import com.metamx.druid.merger.common.TaskStatus;
import com.metamx.druid.merger.common.TaskToolbox;
import com.metamx.druid.merger.coordinator.TaskContext;
import com.metamx.druid.utils.JodaUtils;
import org.codehaus.jackson.annotate.JsonCreator;
import org.codehaus.jackson.annotate.JsonProperty;
import org.joda.time.DateTime;

import java.util.List;

public class HadoopIndexTask extends AbstractTask
{
  @JsonProperty
  private static final Logger log = new Logger(HadoopIndexTask.class);

  @JsonProperty
  private final HadoopDruidIndexerConfig config;

  /**
   * @param config is used by the HadoopDruidIndexerJob to set up the appropriate parameters
   *               for creating Druid index segments. It may be modified.
   *               <p/>
   *               Here, we will ensure that the UpdaterJobSpec field of the config is set to null, such that the
   *               job does not push a list of published segments the database. Instead, we will use the method
   *               IndexGeneratorJob.getPublishedSegments() to simply return a list of the published
   *               segments, and let the indexing service report these segments to the database.
   */

  @JsonCreator
  public HadoopIndexTask(@JsonProperty("config") HadoopDruidIndexerConfig config)
  {
    super(
        String.format("index_hadoop_%s_interval_%s", config.getDataSource(), new DateTime()),
        config.getDataSource(),
        JodaUtils.umbrellaInterval(config.getIntervals())
    );

    if (config.isUpdaterJobSpecSet()) {
      throw new IllegalArgumentException(
          "The UpDaterJobSpec field of the Hadoop Druid indexer config must be set to null "
      );
    }
    this.config = config;
  }

  @Override
  public Type getType()
  {
    return Type.HADOOPINDEX;
  }

  @Override
  public TaskStatus run(TaskContext context, TaskToolbox toolbox) throws Exception
  {
    log.info("Setting version to: %s", context.getVersion());
    config.setVersion(context.getVersion());

    if (toolbox.getSegmentPusher() instanceof S3SegmentPusher) {
      // Hack alert! Bypassing SegmentPusher...
      S3SegmentPusher segmentPusher = (S3SegmentPusher) toolbox.getSegmentPusher();
      String s3Path = String.format(
          "s3n%s/%s/%s",
          segmentPusher.getConfig().getBucket(),
          segmentPusher.getConfig().getBaseKey(),
          getDataSource()
      );

      log.info("Setting segment output path to: %s", s3Path);
      config.setSegmentOutputDir(s3Path);
    } else {
      throw new IllegalStateException("Sorry, we only work with S3SegmentPushers! Bummer!");
    }

    HadoopDruidIndexerJob job = new HadoopDruidIndexerJob(config);
    log.debug("Starting a hadoop index generator job...");

    if (job.run()) {
      List<DataSegment> publishedSegments = job.getPublishedSegments();
      return TaskStatus.success(getId(), ImmutableList.copyOf(publishedSegments));

    } else {
      return TaskStatus.failure(getId());
    }

  }
}
