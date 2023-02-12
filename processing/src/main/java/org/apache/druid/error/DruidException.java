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

package org.apache.druid.error;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an error condition exposed to the user and/or operator of Druid.
 * Not needed for purely internal exceptions thrown and caught within Druid itself.
 * There are categories of error that determine the general form of corrective
 * action, and also determine HTTP (or other API) status codes.
 * <p>
 * Druid exceptions can contain context. Use the context for details, such as
 * file names, query context variables, symbols, etc. This allows the error
 * message itself to be simple. Context allows consumers to filter out various
 * bits of information that a site does not wish to expose to the user, while
 * still logging the full details. Typical usage:
 * <pre><code>
 * if (something_is_wrong) {
 *   throw DruidException.user("File not found")
 *       .context("File name", theFile.getName())
 *       .context("Directory", theFile.getParent())
 *       .build();
 * }
 * </code></pre>
 * <p>
 * Exceptions are immutable. In many cases, an error is thrown low in the code,
 * bit context is known at a higher level. In this case, the higher code should
 * catch the exception, convert back to a builder, add context, and throw the
 * new exception. The original call stack is maintained. Example:
 * <pre><code>
 * catch (DruidExceptin e) {
 *   throw e.toBuilder().
 *       .context("File name", theFile.getName())
 *       .context("Directory", theFile.getParent())
 *       .build();
 * }
 * </code></pre>
 */
public class DruidException extends RuntimeException
{
  public enum ErrorType
  {
    /**
     * General case of an error due to something the user asked to do in an REST
     * request. Translates to an HTTP status 400 (BAD_REQUET) for a REST call
     * (or the equivalent for other APIs.)
     */
    USER,

    /**
     * Special case of a user error where a resource is not found and we wish
     * to return a 404 (NOT_FOUND) HTTP status (or the equivalent for other
     * APIs.)
     */
    NOT_FOUND,

    /**
     * Error due to a problem beyond the user's control, such as an assertion
     * failed, unsupported operation, etc. These indicate problems with the software
     * where the fix is either a workaround or a bug fix. Such error should only
     * be raised for "should never occur" type situations.
     */
    SYSTEM,

    /**
     * Error for a resource limit: memory, CPU, slots or so on. The workaround is
     * generally to try later, get more resources, reduce load or otherwise resolve
     * the resource pressure issue.
     */
    RESOURCE,

    /**
     * Similar to RESOURCE, except indicates a timeout, perhaps due to load, due
     * to an external system being unavailable, etc.
     */
    TIMEOUT,

    /**
     * Error in configuration. Indicates that the administrator made a mistake during
     * configuration or setup. The solution is for the administrator (not the end user)
     * to resolve the issue.
     */
    CONFIG,

    /**
     * Indicates a network error of some kind: intra-Druid, client-to-Druid,
     * Druid-to-external system, etc. Generally the end user cannot fix these errors:
     * it requires a DevOps person to resolve.
     */
    NETWORK
  };

  public static final String ERROR_CODE = "Error Code";
  public static final String HOST = "Host";

  public static class Builder
  {
    private final DruidException source;
    private final ErrorType type;
    private final String msg;
    private Throwable e;
    private Map<String, String> context;

    private Builder(ErrorType type, String msg, Object[] args)
    {
      this.source = null;
      this.type = type;
      this.msg = StringUtils.format(msg, args);
    }

    private Builder(DruidException e)
    {
      this.source = e;
      this.type = e.type;
      this.msg = e.message();
      this.e = e.getCause() == null ? e : e.getCause();
      this.context = e.context == null ? null : new HashMap<>(e.context);
    }

    public Builder cause(Throwable e)
    {
      this.e = e;
      if (!msg.equals(e.getMessage())) {
        context("Cause", e.getMessage());
      }
      return this;
    }

    public Builder context(String key, Object value)
    {
      if (context == null) {
        // Used linked hash map to preserve order
        context = new LinkedHashMap<String, String>();
      }
      context.put(key, value == null ? "" : value.toString());
      return this;
    }

    private boolean wasLogged()
    {
      return source != null && source.logged;
    }

    private DruidException build(boolean logged)
    {
      return new DruidException(
          e,
          msg,
          type,
          // Used linked hash map to preserve order
          context == null ? null : new LinkedHashMap<>(context),
          logged || wasLogged()
      );
    }

    public DruidException build()
    {
      return build(false);
    }

    public DruidException build(Logger logger)
    {
      DruidException e = build(true);
      if (wasLogged()) {
        return e;
      }
      switch (type) {
        case CONFIG:
        case SYSTEM:
          logger.error(e, e.getMessage());
          break;
        case NETWORK:
        case RESOURCE:
          logger.warn(e, e.getMessage());
          break;
        default:
          logger.info(e, e.getMessage());
          break;
      }
      return e;
    }

    @Override
    public String toString()
    {
      return build().getMessage();
    }
  }

  private final ErrorType type;
  private final Map<String, String> context;
  private final boolean logged;

  public DruidException(
      final Throwable e,
      final String msg,
      final ErrorType type,
      final Map<String, String> context,
      final boolean logged
  )
  {
    super(msg, e);
    this.type = type;
    this.context = context;
    this.logged = logged;
  }

  /**
   * Build an error that indicates the user provided incorrect input.
   * The user can correct the error by correcting their input (their query,
   * REST message, etc.)
   */
  public static Builder user(String msg, Object...args)
  {
    return new Builder(ErrorType.USER, msg, args);
  }

  public static DruidException userError(String msg, Object...args)
  {
    return user(msg, args).build();
  }

  /**
   * Build an error that indicates that something went wrong internally
   * with Druid. This is the equivalent of an assertion failure: errors
   * of this type indicate a bug in the code: there is nothing the user
   * can do other than request a fix or find a workaround.
   */
  public static Builder system(String msg, Object...args)
  {
    return new Builder(ErrorType.SYSTEM, msg, args);
  }

  public static Builder notFound(String msg, Object...args)
  {
    return new Builder(ErrorType.NOT_FOUND, msg, args);
  }

  public static DruidException unexpected(Exception e)
  {
    return system(e.getMessage()).cause(e).build();
  }

  /**
   * Build an error that indicates Druid reached some kind of resource limit:
   * memory, disk, CPU, etc. Generally the resolution is to reduce load or
   * add resources to Druid.
   */
  public static Builder resourceError(String msg, Object...args)
  {
    return new Builder(ErrorType.RESOURCE, msg, args);
  }

  public static Builder timeoutError(String msg, Object...args)
  {
    return new Builder(ErrorType.TIMEOUT, msg, args);
  }

  /**
   * Build an error that indicates a configuration error which generally means
   * that Druid won't start until the user corrects a configuration file or
   * similar artifact.
   */
  public static Builder configError(String msg, Object...args)
  {
    return new Builder(ErrorType.CONFIG, msg, args);
  }

  /**
   * Network I/O, connection, timeout or other error that indicates a problem
   * with the client-to-Druid connection, and internal Druid-to-Druid connection,
   * or a Druid-to-External error.
   */
  public static Builder networkError(String msg, Object...args)
  {
    return new Builder(ErrorType.NETWORK, msg, args);
  }

  /**
   * Convert the exception back into a builder, generally so a higher level
   * of code can add more context.
   */
  public Builder toBuilder()
  {
    return new Builder(this);
  }

  public ErrorType type()
  {
    return type;
  }

  public Map<String, String> context()
  {
    return context;
  }

  public String context(String key)
  {
    return context.get(key);
  }

  @Override
  public String getMessage()
  {
    StringBuilder buf = new StringBuilder();
    switch (type) {
      case CONFIG:
        buf.append("Configuration error: ");
        break;
      case RESOURCE:
        buf.append("Resource error: ");
        break;
      case SYSTEM:
        buf.append("System error: ");
        break;
      default:
        break;
    }
    buf.append(super.getMessage());
    if (context != null && context.size() > 0) {
      for (Map.Entry<String, String> entry : context.entrySet()) {
        buf.append("\n")
           .append(entry.getKey())
           .append(": ")
           .append(entry.getValue());
      }
    }
    return buf.toString();
  }

  public String message()
  {
    return super.getMessage();
  }
}
