/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.security.opa;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.security.opa.opatypes.OpaMessage;
import org.apache.druid.security.opa.opatypes.OpaResponse;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.Resource;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;

@JsonTypeName("opa")
public class OpaAuthorizer implements Authorizer
{
  private static final Logger LOG = new Logger(OpaAuthorizer.class);
  private final URI opaUri;
  private final ObjectMapper objectMapper;
  private final HttpClient httpClient;

  @JsonCreator
  public OpaAuthorizer(
      @JsonProperty("name") String name,
      @JsonProperty("opaUri") String opaUri
  )
  {
    try {
      this.opaUri = new URI(opaUri);
    }
    catch (Exception e) {
      throw new IllegalArgumentException("Invalid opaUri: " + opaUri, e);
    }
    this.objectMapper =
        new ObjectMapper()
            // https://github.com/stackabletech/druid-opa-authorizer/issues/72
            // OPA server can send other fields, such as `decision_id`` when enabling decision logs
            // We could add all the fields we *currently* know, but it's more future-proof to ignore
            // any unknown fields.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    this.httpClient = HttpClient.newHttpClient();
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult,
      Resource resource,
      Action action
  )
  {
    LOG.debug(
        "Authorizing %s for %s on %s",
        authenticationResult.getIdentity(),
        action.name(),
        resource.toString()
    );
    LOG.trace("Creating OPA request JSON.");
    OpaMessage msg = new OpaMessage(
        authenticationResult,
        action.name(),
        resource.getName(),
        resource.getType()
    );
    String msgJson;
    try {
      msgJson = objectMapper.writeValueAsString(msg);
    }
    catch (JsonProcessingException e) {
      return new Access(false, "Failed to create the OPA request JSON: " + e);
    }

    LOG.trace("Executing post to OPA.");
    try {
      HttpRequest request =
          HttpRequest.newBuilder()
                     .uri(opaUri)
                     .header("Content-Type", "application/json")
                     .POST(HttpRequest.BodyPublishers.ofString(msgJson))
                     .build();

      HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      LOG.debug("OPA Response code: %s - %s", response.statusCode(), response.body());
      LOG.trace("Parsing OPA response.");
      OpaResponse opaResponse = objectMapper.readValue(response.body(), OpaResponse.class);
      if (opaResponse.isResult()) {
        return Access.OK;
      } else {
        return new Access(false, "Access denied.");
      }

    }
    catch (Exception e) {
      return new Access(false, "An error occurred: " + e);
    }
  }
}
