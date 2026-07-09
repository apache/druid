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

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchResult;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

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
    this(name, opaUri, HttpClient.newHttpClient());
  }

  public OpaAuthorizer(
      String name,
      String opaUri,
      HttpClient httpClient
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
            // OPA server can send other fields, such as `decision_id` when enabling decision logs
            // We could add all the fields we *currently* know, but it's more future-proof to ignore
            // any unknown fields.
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    this.httpClient = httpClient;
    // name is required for @JsonCreator but unused in this implementation
    LOG.debug("Created OpaAuthorizer [%s]", name);
  }

  @Override
  public Access authorize(
      AuthenticationResult authenticationResult,
      Resource resource,
      Action action
  )
  {
    LOG.debug(
        "Authorizing [%s] for [%s] on [%s]",
        authenticationResult.getIdentity(),
        action.name(),
        resource.toString()
    );

    final AuthenticationResult sanitizedAuthResult = new AuthenticationResult(
        authenticationResult.getIdentity(),
        authenticationResult.getAuthorizerName(),
        authenticationResult.getAuthenticatedBy(),
        sanitizeContext(authenticationResult.getContext())
    );

    LOG.trace("Creating OPA request JSON.");
    final OpaMessage msg = new OpaMessage(
        sanitizedAuthResult,
        action.name(),
        resource.getName(),
        resource.getType()
    );
    final String msgJson;
    try {
      msgJson = objectMapper.writeValueAsString(msg);
    }
    catch (JsonProcessingException e) {
      return Access.deny("Failed to create the OPA request JSON: " + e);
    }

    LOG.trace("Executing post to OPA.");
    try {
      final HttpRequest request =
          HttpRequest.newBuilder()
                     .uri(opaUri)
                     .header("Content-Type", "application/json")
                     .POST(HttpRequest.BodyPublishers.ofString(msgJson))
                     .build();

      final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

      LOG.debug("OPA response code [%s]", response.statusCode());
      LOG.trace("OPA response body [%s]", response.body());

      if (response.statusCode() != HttpURLConnection.HTTP_OK) {
        return Access.deny(
            "OPA request failed with status code [" + response.statusCode() + "]"
        );
      }

      LOG.trace("Parsing OPA response.");
      final OpaResponse opaResponse = objectMapper.readValue(response.body(), OpaResponse.class);
      if (opaResponse.isResult()) {
        return Access.OK;
      } else {
        return Access.DENIED;
      }

    }
    catch (Exception e) {
      return Access.deny("An error occurred: " + e);
    }
  }

  protected Map<String, Object> sanitizeContext(Map<String, Object> context)
  {
    if (context == null || context.isEmpty()) {
      return context;
    }

    final Map<String, Object> sanitizedContext = new HashMap<>();
    for (final Map.Entry<String, Object> entry : context.entrySet()) {
      if (entry.getValue() instanceof SearchResult) {
        try {
          sanitizedContext.put(entry.getKey(), sanitizeSearchResult((SearchResult) entry.getValue()));
        }
        catch (NamingException e) {
          LOG.warn(e, "Failed to sanitize SearchResult in context key [%s]", entry.getKey());
          sanitizedContext.put(entry.getKey(), Collections.emptyMap());
        }
      } else {
        // Keep other types as is, assuming they are serializable or handled by other means
        sanitizedContext.put(entry.getKey(), entry.getValue());
      }
    }
    return sanitizedContext;
  }

  private Map<String, Object> sanitizeSearchResult(SearchResult searchResult) throws NamingException
  {
    final Map<String, Object> sanitized = new HashMap<>();
    sanitized.put("name", searchResult.getName());

    try {
      sanitized.put("nameInNamespace", searchResult.getNameInNamespace());
    }
    catch (UnsupportedOperationException e) {
      // SearchResult.getNameInNamespace() throws UnsupportedOperationException if the result is relative
      // and the full name hasn't been set by the context. It's safe to ignore.
    }

    final Map<String, List<Object>> attributesMap = new HashMap<>();
    final Attributes attributes = searchResult.getAttributes();
    if (attributes != null) {
      final NamingEnumeration<? extends Attribute> attrEnum = attributes.getAll();
      try {
        while (attrEnum != null && attrEnum.hasMore()) {
          final Attribute attr = attrEnum.next();
          final List<Object> values = new ArrayList<>();
          final NamingEnumeration<?> valueEnum = attr.getAll();
          try {
            while (valueEnum != null && valueEnum.hasMore()) {
              final Object val = valueEnum.next();
              if (val instanceof byte[]) {
                values.add(Base64.getEncoder().encodeToString((byte[]) val));
              } else if (val != null) {
                values.add(val.toString());
              }
            }
          }
          finally {
            if (valueEnum != null) {
              valueEnum.close();
            }
          }
          attributesMap.put(attr.getID().toLowerCase(Locale.ENGLISH), values);
        }
      }
      finally {
        if (attrEnum != null) {
          attrEnum.close();
        }
      }
    }
    sanitized.put("attributes", attributesMap);
    return sanitized;
  }
}
