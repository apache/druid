package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import org.apache.druid.data.input.impl.ByteEntity;
import org.apache.druid.indexing.common.task.TaskResource;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTask;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskRunner;
import org.apache.druid.indexing.seekablestream.common.RecordSupplier;
import org.apache.druid.segment.indexing.DataSchema;
import org.apache.druid.segment.indexing.TuningConfig;

import java.util.Map;

public class PulsarIndexTask extends SeekableStreamIndexTask<Integer, Long, ByteEntity>
{

  private static final String TYPE = "index_pulsar";
  private final PulsarIndexTaskIOConfig ioConfig;


  @JsonCreator
  public PulsarIndexTask(
      @JsonProperty("id") String id,
      @JsonProperty("resource") TaskResource taskResource,
      @JsonProperty("dataSchema") DataSchema dataSchema,
      @JsonProperty("tuningConfig") PulsarIndexTaskTuningConfig tuningConfig,
      @JsonProperty("ioConfig") PulsarIndexTaskIOConfig ioConfig,
      @JsonProperty("context") Map<String, Object> context,
      @JacksonInject ObjectMapper configMapper
  )
  {
    super(
        getOrMakeId(id, dataSchema.getDataSource(), TYPE),
        taskResource,
        dataSchema,
        tuningConfig,
        ioConfig,
        context,
        getFormattedGroupId(dataSchema.getDataSource(), TYPE)
    );
    this.ioConfig = ioConfig;

    Preconditions.checkArgument(
        ioConfig.getStartSequenceNumbers().getExclusivePartitions().isEmpty(),
        "All startSequenceNumbers must be inclusive"
    );

  }

  @Override
  public String getType()
  {
    return null;
  }

  @Override
  @JsonProperty
  public PulsarIndexTaskTuningConfig getTuningConfig()
  {
    return (PulsarIndexTaskTuningConfig) super.getTuningConfig();
  }

  @Override
  @JsonProperty("ioConfig")
  public PulsarIndexTaskIOConfig getIOConfig()
  {
    return (PulsarIndexTaskIOConfig) super.getIOConfig();
  }

  @Override
  protected SeekableStreamIndexTaskRunner<Integer, Long, ByteEntity> createTaskRunner()
  {
    //noinspection unchecked
    return new PulsarIndexTaskRunner(
        this,
        dataSchema.getParser(),
        authorizerMapper,
        lockGranularityToUse
    );
  }

  @Override
  protected RecordSupplier<Integer, Long, ByteEntity> newTaskRecordSupplier()
  {
    ClassLoader currCtxCl = Thread.currentThread().getContextClassLoader();
    try {
      Thread.currentThread().setContextClassLoader(getClass().getClassLoader());

      int maxRowsInMemory = TuningConfig.DEFAULT_MAX_ROWS_IN_MEMORY;

      if (tuningConfig != null) {
        maxRowsInMemory = tuningConfig.getMaxRowsInMemory();
      }

      return new PulsarRecordSupplier(
          ioConfig.getServiceUrl(),
          getId(),
          maxRowsInMemory,
          ioConfig.getAuthPluginClassName(),
          ioConfig.getAuthParams(),
          ioConfig.getOperationTimeoutMs(),
          ioConfig.getStatsIntervalSeconds(),
          ioConfig.getNumIoThreads(),
          ioConfig.getNumListenerThreads(),
          ioConfig.getUseTcpNoDelay(),
          ioConfig.getUseTls(),
          ioConfig.getTlsTrustCertsFilePath(),
          ioConfig.getTlsAllowInsecureConnection(),
          ioConfig.getTlsHostnameVerificationEnable(),
          ioConfig.getConcurrentLookupRequest(),
          ioConfig.getMaxLookupRequest(),
          ioConfig.getMaxNumberOfRejectedRequestPerConnection(),
          ioConfig.getKeepAliveIntervalSeconds(),
          ioConfig.getConnectionTimeoutMs(),
          ioConfig.getRequestTimeoutMs(),
          ioConfig.getMaxBackoffIntervalNanos()
      );
    } finally {
      Thread.currentThread().setContextClassLoader(currCtxCl);
    }
  }

  @Override
  public boolean supportsQueries()
  {
    return true;
  }
}
