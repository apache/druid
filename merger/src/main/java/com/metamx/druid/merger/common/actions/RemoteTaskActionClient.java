package com.metamx.druid.merger.common.actions;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.ISE;
import com.metamx.common.logger.Logger;
import com.metamx.druid.merger.common.task.Task;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.ToStringResponseHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.curator.x.discovery.ServiceInstance;
import com.netflix.curator.x.discovery.ServiceProvider;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

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
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    try {
      byte[] dataToSend = jsonMapper.writeValueAsBytes(new TaskActionHolder(task, taskAction));

      final String response = httpClient.post(getServiceUri().toURL())
                                        .setContent("application/json", dataToSend)
                                        .go(new ToStringResponseHandler(Charsets.UTF_8))
                                        .get();

      final Map<String, Object> responseDict = jsonMapper.readValue(
          response,
          new TypeReference<Map<String, Object>>() {}
      );

      return jsonMapper.convertValue(responseDict.get("result"), taskAction.getReturnTypeReference());
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private URI getServiceUri() throws Exception
  {
    final ServiceInstance instance = serviceProvider.getInstance();
    final String scheme;
    final String host;
    final int port;
    final String path = "/mmx/merger/v1/action";

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
