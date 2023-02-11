package org.apache.druid.error;

import javax.ws.rs.core.Response;

public interface RestExceptionEncoder
{
  Response encode(DruidException e);
  Response.ResponseBuilder builder(DruidException e);
}
