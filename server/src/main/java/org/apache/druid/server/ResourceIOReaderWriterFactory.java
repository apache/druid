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


package org.apache.druid.server;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.joda.ser.DateTimeSerializer;
import com.fasterxml.jackson.jaxrs.smile.SmileMediaTypes;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.inject.Inject;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryInterruptedException;
import org.apache.druid.query.QueryToolChest;
import org.joda.time.DateTime;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;

/**
 * Factory for creating instances of {@link ResourceIOReaderWriter}.
 */
public class ResourceIOReaderWriterFactory
{

  private final ObjectMapper jsonMapper;
  private final ObjectMapper smileMapper;
  private final ObjectMapper serializeDateTimeAsLongJsonMapper;
  private final ObjectMapper serializeDateTimeAsLongSmileMapper;

  @Inject
  public ResourceIOReaderWriterFactory(
      @Json ObjectMapper jsonMapper,
      @Smile ObjectMapper smileMapper
  )
  {
    this.jsonMapper = jsonMapper;
    this.smileMapper = smileMapper;
    this.serializeDateTimeAsLongJsonMapper = serializeDataTimeAsLong(jsonMapper);
    this.serializeDateTimeAsLongSmileMapper = serializeDataTimeAsLong(smileMapper);
  }

  /**
   * Creates a {@link ResourceIOReaderWriter} instance. The response writer is based on request accept header, with a
   * fallback to content type if the accept header is not provided.
   */
  public ResourceIOReaderWriter factorize(HttpServletRequest req, boolean pretty)
  {
    String requestType = req.getContentType();
    String acceptHeader = req.getHeader("Accept");

    // response type defaults to Content-Type if 'Accept' header not provided
    String responseType = Strings.isNullOrEmpty(acceptHeader) ? requestType : acceptHeader;

    boolean isRequestSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(requestType)
                             || QueryResource.APPLICATION_SMILE.equals(requestType);

    boolean isResponseSmile = SmileMediaTypes.APPLICATION_JACKSON_SMILE.equals(responseType)
                              || QueryResource.APPLICATION_SMILE.equals(responseType);

    return new ResourceIOReaderWriter(
        isRequestSmile ? smileMapper : jsonMapper,
        new ResourceIOReaderWriterFactory.ResourceIOWriter(
            isResponseSmile ? SmileMediaTypes.APPLICATION_JACKSON_SMILE : MediaType.APPLICATION_JSON,
            isResponseSmile ? smileMapper : jsonMapper,
            isResponseSmile ? serializeDateTimeAsLongSmileMapper : serializeDateTimeAsLongJsonMapper,
            pretty
        )
    );
  }

  private static ObjectMapper serializeDataTimeAsLong(ObjectMapper mapper)
  {
    return mapper.copy().registerModule(new SimpleModule().addSerializer(DateTime.class, new DateTimeSerializer()));
  }

  /**
   * Encapsulates the mapper for the request and the {@link ResourceIOWriter} for the response.
   */
  public static class ResourceIOReaderWriter
  {
    private final ObjectMapper requestMapper;
    private final ResourceIOWriter writer;

    public ResourceIOReaderWriter(ObjectMapper requestMapper, ResourceIOWriter writer)
    {
      this.requestMapper = requestMapper;
      this.writer = writer;
    }

    public ObjectMapper getRequestMapper()
    {
      return requestMapper;
    }

    public ResourceIOWriter getResponseWriter()
    {
      return writer;
    }
  }

  /**
   * Handles writing query response to the client in different formats.
   */
  public static class ResourceIOWriter
  {
    private final String responseType;
    private final ObjectMapper inputMapper;
    private final ObjectMapper serializeDateTimeAsLongInputMapper;
    private final boolean isPretty;

    ResourceIOWriter(
        String responseType,
        ObjectMapper inputMapper,
        ObjectMapper serializeDateTimeAsLongInputMapper,
        boolean isPretty
    )
    {
      this.responseType = responseType;
      this.inputMapper = inputMapper;
      this.serializeDateTimeAsLongInputMapper = serializeDateTimeAsLongInputMapper;
      this.isPretty = isPretty;
    }

    String getResponseType()
    {
      return responseType;
    }

    /**
     * Creates a mapper for writing output.
     */
    ObjectMapper newOutputWriter(
        @Nullable QueryToolChest<?, Query<?>> toolChest,
        @Nullable Query<?> query,
        boolean serializeDateTimeAsLong
    )
    {
      final ObjectMapper mapper = serializeDateTimeAsLong ? serializeDateTimeAsLongInputMapper : inputMapper;
      final ObjectMapper decoratedMapper;
      if (toolChest != null) {
        decoratedMapper = toolChest.decorateObjectMapper(mapper, Preconditions.checkNotNull(query, "query"));
      } else {
        decoratedMapper = mapper;
      }
      return isPretty ? decoratedMapper.copy().enable(SerializationFeature.INDENT_OUTPUT) : decoratedMapper;
    }

    /**
     * Builds a {@link Response} with ok status and the given object serialized to the response format.
     */
    public Response ok(Object object) throws IOException
    {
      return Response.ok(newOutputWriter(null, null, false).writeValueAsString(object), responseType).build();
    }

    /**
     * Builds a {@link Response} with internal server error status and the given exception.
     */
    public Response gotError(Exception e) throws IOException
    {
      return buildNonOkResponse(
          Response.Status.INTERNAL_SERVER_ERROR.getStatusCode(),
          QueryInterruptedException.wrapIfNeeded(e)
      );
    }

    /**
     * Builds a {@link Response} with the given status and the exception.
     */
    public Response buildNonOkResponse(int status, Exception e) throws JsonProcessingException
    {
      return Response.status(status)
                     .type(responseType)
                     .entity(newOutputWriter(null, null, false).writeValueAsBytes(e))
                     .build();
    }
  }
}
