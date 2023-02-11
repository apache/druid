package org.apache.druid.error;

import java.util.HashMap;
import java.util.Map;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

import com.google.common.collect.ImmutableMap;

@SuppressWarnings("serial")
public class DruidException extends RuntimeException
{
  public enum ErrorType
  {
    USER,
    SYSTEM,
    RESOURCE,
    CONFIG,
    NETWORK
  };

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
      this.msg = e.baseMessage();
      this.e = e.getCause() == null ? e : e.getCause();
      this.context = e.context == null ? null : new HashMap<>(e.context);
    }

    public Builder cause(Exception e)
    {
      this.e = e;
      return this;
    }

    public Builder context(String key, Object value)
    {
      if (context == null) {
        context = new HashMap<String, String>();
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
          context == null ? null : ImmutableMap.copyOf(context),
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

  @Override
  public String getMessage()
  {
    StringBuilder buf = new StringBuilder();
    switch (type)
    {
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

  private String baseMessage()
  {
    return super.getMessage();
  }
}
