package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.EscalatedGlobal;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClientFactory;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public class PulsarIndexTaskClientFactory extends SeekableStreamIndexTaskClientFactory<PulsarIndexTaskClient>
{
  @Inject
  public PulsarIndexTaskClientFactory(
      @EscalatedGlobal HttpClient httpClient,
      @Json ObjectMapper mapper
  )
  {
    super(httpClient, mapper);
  }

  @Override
  public PulsarIndexTaskClient build(
      TaskInfoProvider taskInfoProvider,
      String dataSource,
      int numThreads,
      Duration httpTimeout,
      long numRetries
  )
  {
    return new PulsarIndexTaskClient(
        getHttpClient(),
        getMapper(),
        taskInfoProvider,
        dataSource,
        numThreads,
        httpTimeout,
        numRetries
    );
  }
}
