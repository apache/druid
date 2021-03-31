package org.apache.druid.server.initialization.jetty;

/**
 * This class is for any exceptions that should return a Service unavailable status code (503).
 * See {@code BadQueryException} for query requests.
 *
 * @see ServiceUnavailableExceptionMapper
 */
public class ServiceUnavailableException extends RuntimeException
{
  public ServiceUnavailableException(String msg)
  {
    super(msg);
  }
}
