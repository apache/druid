package com.metamx.druid.indexing.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.indexing.common.RetryPolicy;
import com.metamx.druid.indexing.common.RetryPolicyFactory;
import com.metamx.druid.indexing.common.task.Task;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.curator.x.discovery.ServiceInstance;
import org.apache.curator.x.discovery.ServiceProvider;
import org.joda.time.Duration;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final HttpClient httpClient;
  private final ServiceProvider serviceProvider;
  private final RetryPolicyFactory retryPolicyFactory;
  private final ObjectMapper jsonMapper;

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(
      Task task,
      HttpClient httpClient,
      ServiceProvider serviceProvider,
      RetryPolicyFactory retryPolicyFactory,
      ObjectMapper jsonMapper
  )
  {
    this.task = task;
    this.httpClient = httpClient;
    this.serviceProvider = serviceProvider;
    this.retryPolicyFactory = retryPolicyFactory;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    log.info("Performing action for task[%s]: %s", task.getId(), taskAction);

    byte[] dataToSend = jsonMapper.writeValueAsBytes(new TaskActionHolder(task, taskAction));

    final RetryPolicy retryPolicy = retryPolicyFactory.makeRetryPolicy();

    while (true) {
      try {
        final URI serviceUri;
        try {
          serviceUri = getServiceUri();
        }
        catch (Exception e) {
          throw new IOException("Failed to locate service uri", e);
        }

        final String response;

        try {
          response = httpClient.post(serviceUri.toURL())
                               .setContent("application/json", dataToSend)
                               .go(new ToStringResponseHandler(Charsets.UTF_8))
                               .get();
        }
        catch (Exception e) {
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          throw Throwables.propagate(e);
        }

        final Map<String, Object> responseDict = jsonMapper.readValue(
            response,
            new TypeReference<Map<String, Object>>() {}
        );

        return jsonMapper.convertValue(responseDict.get("result"), taskAction.getReturnTypeReference());
      } catch(IOException e) {
        log.warn(e, "Exception submitting action for task: %s", task.getId());

        if (retryPolicy.hasExceededRetryThreshold()) {
          throw e;
        } else {
          try {
            final long sleepTime = retryPolicy.getAndIncrementRetryDelay().getMillis();
            log.info("Will try again in %s.", new Duration(sleepTime).toString());
            Thread.sleep(sleepTime);
          }
          catch (InterruptedException e2) {
            throw Throwables.propagate(e2);
          }
        }
      }
    }
  }

  private URI getServiceUri() throws Exception
  {
    final ServiceInstance instance = serviceProvider.getInstance();
    final String scheme;
    final String host;
    final int port;
    final String path = "/druid/indexer/v1/action";

    if (instance == null) {
      throw new ISE("Cannot find instance of indexer to talk to!");
    }

    host = instance.getAddress();

    if (instance.getSslPort() != null && instance.getSslPort() > 0) {
      scheme = "https";
      port = instance.getSslPort();
    } else {
      scheme = "http";
      port = instance.getPort();
    }

    return new URI(scheme, null, host, port, path, null, null);
  }
}
