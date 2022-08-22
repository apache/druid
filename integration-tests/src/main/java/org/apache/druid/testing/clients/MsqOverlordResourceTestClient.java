package org.apache.druid.testing.clients;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.http.client.HttpClient;
import org.apache.druid.java.util.http.client.response.StatusResponseHolder;
import org.apache.druid.msq.guice.MSQIndexingModule;
import org.apache.druid.msq.indexing.report.MSQTaskReport;
import org.apache.druid.testing.IntegrationTestingConfig;
import org.apache.druid.testing.guice.TestClient;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.util.HashMap;
import java.util.Map;

public class MsqOverlordResourceTestClient extends OverlordResourceTestClient
{
  ObjectMapper jsonMapper;

  @Inject
  MsqOverlordResourceTestClient(
      @Json ObjectMapper jsonMapper,
      @TestClient HttpClient httpClient,
      IntegrationTestingConfig config
  )
  {
    super(jsonMapper, httpClient, config);
    this.jsonMapper = jsonMapper;
  }

  public Map<String, MSQTaskReport> getTaskReportForMsqTask(String taskId)
  {
    try {
      StatusResponseHolder response = makeRequest(
          HttpMethod.GET,
          StringUtils.format(
              "%s%s",
              getIndexerURL(),
              StringUtils.format("task/%s/reports", StringUtils.urlEncode(taskId))
          )
      );
      return jsonMapper.registerModules(new MSQIndexingModule().getJacksonModules()).readValue(
          response.getContent(),
          new TypeReference<Map<String, MSQTaskReport>>()
          {
          }
      );
    }
    catch (ISE e) {
      throw e;
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static class MsqTaskReportType extends HashMap<String, MSQTaskReport>
  {

  }
}
