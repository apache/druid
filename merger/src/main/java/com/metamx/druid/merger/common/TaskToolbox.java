package com.metamx.druid.merger.common;

import com.google.common.collect.ImmutableMap;
import com.metamx.druid.loading.S3SegmentGetter;
import com.metamx.druid.loading.S3SegmentGetterConfig;
import com.metamx.druid.loading.S3ZippedSegmentGetter;
import com.metamx.druid.loading.SegmentGetter;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.druid.merger.coordinator.config.IndexerCoordinatorConfig;
import com.metamx.druid.realtime.SegmentPusher;
import com.metamx.emitter.service.ServiceEmitter;
import org.codehaus.jackson.map.ObjectMapper;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;

import java.io.File;
import java.util.Map;

/**
 * Stuff that may be needed by a Task in order to conduct its business.
 */
public class TaskToolbox
{
  private final IndexerCoordinatorConfig config;
  private final ServiceEmitter emitter;
  private final RestS3Service s3Client;
  private final SegmentPusher segmentPusher;
  private final ObjectMapper objectMapper;

  public TaskToolbox(
      IndexerCoordinatorConfig config,
      ServiceEmitter emitter,
      RestS3Service s3Client,
      SegmentPusher segmentPusher,
      ObjectMapper objectMapper
  )
  {
    this.config = config;
    this.emitter = emitter;
    this.s3Client = s3Client;
    this.segmentPusher = segmentPusher;
    this.objectMapper = objectMapper;
  }

  public IndexerCoordinatorConfig getConfig()
  {
    return config;
  }

  public ServiceEmitter getEmitter()
  {
    return emitter;
  }

  public RestS3Service getS3Client()
  {
    return s3Client;
  }

  public SegmentPusher getSegmentPusher()
  {
    return segmentPusher;
  }

  public ObjectMapper getObjectMapper()
  {
    return objectMapper;
  }

  public Map<String, SegmentGetter> getSegmentGetters(final Task task)
  {
    final S3SegmentGetterConfig getterConfig = new S3SegmentGetterConfig()
    {
      @Override
      public File getCacheDirectory()
      {
        return new File(config.getTaskDir(task), "fetched_segments");
      }
    };

    return ImmutableMap.<String, SegmentGetter>builder()
                       .put("s3", new S3SegmentGetter(s3Client, getterConfig))
                       .put("s3_union", new S3SegmentGetter(s3Client, getterConfig))
                       .put("s3_zip", new S3ZippedSegmentGetter(s3Client, getterConfig))
                       .build();
  }
}
