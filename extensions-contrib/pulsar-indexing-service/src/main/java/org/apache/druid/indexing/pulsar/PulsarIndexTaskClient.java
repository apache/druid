package org.apache.druid.indexing.pulsar;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.indexing.common.TaskInfoProvider;
import org.apache.druid.indexing.seekablestream.SeekableStreamIndexTaskClient;
import org.apache.druid.java.util.http.client.HttpClient;
import org.joda.time.Duration;

public class PulsarIndexTaskClient extends SeekableStreamIndexTaskClient<Integer, Long> {
  public PulsarIndexTaskClient(HttpClient httpClient,
                               ObjectMapper jsonMapper,
                               TaskInfoProvider taskInfoProvider,
                               String dataSource, int numThreads, Duration httpTimeout,
                               long numRetries) {
    super(httpClient, jsonMapper, taskInfoProvider, dataSource, numThreads, httpTimeout, numRetries);
  }

  @Override
  protected Class<Integer> getPartitionType() {
    return Integer.class;
  }

  @Override
  protected Class<Long> getSequenceType() {
    return Long.class;
  }
}
