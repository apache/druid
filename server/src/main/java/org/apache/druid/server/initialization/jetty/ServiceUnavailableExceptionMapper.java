package org.apache.druid.server.initialization.jetty;

import com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class ServiceUnavailableExceptionMapper implements ExceptionMapper<ServiceUnavailableException>
{
  @Override
  public Response toResponse(ServiceUnavailableException exception)
  {
    return Response.status(Response.Status.SERVICE_UNAVAILABLE)
                   .type(MediaType.APPLICATION_JSON)
                   .entity(ImmutableMap.of("error", exception.getMessage()))
                   .build();
  }
}
