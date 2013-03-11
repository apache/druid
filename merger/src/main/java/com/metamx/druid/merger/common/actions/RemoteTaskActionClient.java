package com.metamx.druid.merger.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProvider;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final Task task;
  private final HttpClient httpClient;
  private final ServiceProvider serviceProvider;
  private final ObjectMapper jsonMapper;

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(Task task, HttpClient httpClient, ServiceProvider serviceProvider, ObjectMapper jsonMapper)
  {
    this.task = task;
    this.httpClient = httpClient;
    this.serviceProvider = serviceProvider;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction) throws IOException
  {
    byte[] dataToSend = jsonMapper.writeValueAsBytes(new TaskActionHolder(task, taskAction));

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
  }

  private URI getServiceUri() throws Exception
  {
    final ServiceInstance instance = serviceProvider.getInstance();
    final String scheme;
    final String host;
    final int port;
    final String path = "/mmx/merger/v1/action";

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
