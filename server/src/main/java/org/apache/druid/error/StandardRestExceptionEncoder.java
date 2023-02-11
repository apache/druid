package org.apache.druid.error;

import com.google.common.collect.ImmutableMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

public class StandardRestExceptionEncoder implements RestExceptionEncoder
{
  private static final RestExceptionEncoder instance = new StandardRestExceptionEncoder();

  public static RestExceptionEncoder instance()
  {
    return instance;
  }

  @Override
  public ResponseBuilder builder(DruidException e)
  {
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    builder.put("error", errorCode(e));
    builder.put("message", e.message());
    if (e.context() != null) {
      builder.put("context", ImmutableMap.copyOf(e.context()));
    }
    return Response
      .status(status(e))
      .entity(builder.build());
  }

  @Override
  public Response encode(DruidException e)
  {
    return builder(e).build();
  }

  private Object errorCode(DruidException e)
  {
    return e.type().name();
  }

  // Temporary status mapping
  private Status status(DruidException e)
  {
    switch (e.type()) {
    case CONFIG:
    case SYSTEM:
    case NETWORK:
      return Response.Status.INTERNAL_SERVER_ERROR;
    case NOT_FOUND:
      return Response.Status.NOT_FOUND;
    case RESOURCE:
      return Response.Status.SERVICE_UNAVAILABLE;
    case USER:
      return Response.Status.BAD_REQUEST;
    default:
      // Should never occur
      return Response.Status.INTERNAL_SERVER_ERROR;
    }
  }
}
