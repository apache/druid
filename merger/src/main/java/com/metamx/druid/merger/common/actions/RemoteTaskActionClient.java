package com.metamx.druid.merger.common.actions;

import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.metamx.common.logger.Logger;
import com.metamx.http.client.HttpClient;
import com.metamx.http.client.response.ToStringResponseHandler;
import org.codehaus.jackson.map.ObjectMapper;

import java.net.URI;
import java.net.URISyntaxException;

public class RemoteTaskActionClient implements TaskActionClient
{
  private final HttpClient httpClient;
  private final ObjectMapper jsonMapper;

  private static final Logger log = new Logger(RemoteTaskActionClient.class);

  public RemoteTaskActionClient(HttpClient httpClient, ObjectMapper jsonMapper)
  {
    this.httpClient = httpClient;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public <RetType> RetType submit(TaskAction<RetType> taskAction)
  {
    try {
      byte[] dataToSend = jsonMapper.writeValueAsBytes(taskAction);

      final String response = httpClient.post(getServiceUri().toURL())
                                        .setContent("application/json", dataToSend)
                                        .go(new ToStringResponseHandler(Charsets.UTF_8))
                                        .get();

      // TODO Figure out how to check HTTP status code
      if(response.equals("")) {
        return null;
      } else {
        return jsonMapper.readValue(response, taskAction.getReturnTypeReference());
      }
    }
    catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  public URI getServiceUri() throws URISyntaxException
  {
    return new URI("http://localhost:8087/mmx/merger/v1/action");
  }
}
