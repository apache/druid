package org.apache.druid.error;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Represents an error condition exposed to the user and/or operator of Druid.
 * Each error category is given by a subclass of this class.
 * <p>
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
 *   throw new NotFoundException("File not found")
 *       .addContext("File name", theFile.getName())
 *       .addContext("Directory", theFile.getParent());
 * }
 * </code></pre>
 * <p>
 * Exceptions are mutable and may not be modified by two thread concurrently.
 * However, it is highly unlikely that such concurrent access would occur: that's
 * not how exceptions work. Exceptions can be exchanged across threads, as long
 * as only one thread at a time mutates the exception.
 * <p>
 * Druid exceptions allow the calling method (or thread) to add context and set
 * the host name. It is often easier for a higher-level method to fill in this
 * Information than to pass the information into every method. For example:
 * <pre><code>
 * void doTheRead(Reader reader) {
 *   try {
 *      // read some stuff
 *   } catch (IOException e) {
 *     throw new DruidIOException(e);
 *   }
 * }
 *
 * void outer(File theFile) {
 *   try (Reader reader = open(theFile)) {
 *     doTheRead(reader)
 *   } catch (DruidException e) {
 *      throw e.addContext("File name", theFile.getName());
 *   }
 * }
 * </code></pre>
 */
@NotThreadSafe
public abstract class DruidException extends RuntimeException
{
  /**
   * The context provides additional information about an exception which may
   * be redacted on a managed system. Provide essential information in the
   * message itself.
   */
  // Linked hash map to preserve order
  private Map<String, String> context;

  /**
   * Name of the host on which the error occurred, when the error occurred on
   * a host other than the one to which the original request was sent. For example,
   * in a query, if the error occurs on a historical, this field names that historical.
   */
  private String host;

  /**
   * Good errors provide a suggestion to resolve the issue. Such suggestions should
   * focus on a simple Druid installation where the user is also the admin. More
   * advanced deployments may find such helpful suggestions to be off the mark.
   * To resolve this conflict, add suggestions separately from the message itself
   * so each consumer can decide whether to include it or not.
   */
  private String suggestion;

  public DruidException(
      final String msg,
      @Nullable final Object...args)
  {
    super(StringUtils.format(msg, args));
  }

  public DruidException(
      final Throwable cause,
      final String msg,
      @Nullable final Object...args)
  {
    super(StringUtils.format(msg, args), cause);
  }

  public DruidException setHost(String host)
  {
    this.host = host;
    return this;
  }

  @JsonProperty
  public String getErrorClass()
  {
    String errorClass = errorClass();
    return errorClass == null ? getClass().getName() : errorClass;
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public String getHost()
  {
    return host;
  }

  public DruidException suggestion(String suggestion)
  {
    this.suggestion = suggestion;
    return this;
  }

  public String getSuggestion()
  {
    return suggestion;
  }

  public DruidException addContext(String key, String value)
  {
    if (context == null) {
      context = new LinkedHashMap<>();
    }
    context.put(key, value);
    return this;
  }

  public abstract ErrorCategory category();

  public String errorClass()
  {
    return getClass().getName();
  }

  /**
   * The error code is a summary of the error returned to the user. Multiple errors
   * map to the same code: the code is more like a category of errors. Error codes
   * must be backward compatible, even if the prior "codes" are awkward.
   */
  @Nullable
  @JsonProperty("error")
  public String getErrorCode()
  {
    return category().userText();
  }

  public String message()
  {
    return super.getMessage();
  }

  @JsonProperty("errorMessage")
  @Override
  public String getMessage()
  {
    StringBuilder buf = new StringBuilder();
    buf.append(super.getMessage());
    String sep = "; ";
    if (context != null && context.size() > 0) {
      for (Map.Entry<String, String> entry : context.entrySet()) {
        buf.append(sep);
        sep = ", ";
        buf.append("\n")
           .append(entry.getKey())
           .append(": [")
           .append(entry.getValue())
           .append("]");
      }
    }
    if (!Strings.isNullOrEmpty(host)) {
      buf.append(sep).append("Host: ").append(host);
    }
    return buf.toString();
  }

  public String getDisplayMessage()
  {
    StringBuilder buf = new StringBuilder();
    String prefix = category().prefix();
    if (!Strings.isNullOrEmpty(prefix)) {
      buf.append(prefix).append(" - ");
    }
    buf.append(super.getMessage());
    buf.append("\nError Code: ").append(category().userText());
    if (context != null && context.size() > 0) {
      for (Map.Entry<String, String> entry : context.entrySet()) {
        buf.append("\n")
           .append(entry.getKey())
           .append(": ")
           .append(entry.getValue());
      }
    }
    if (!Strings.isNullOrEmpty(host)) {
      buf.append("\nHost: ").append(host);
    }
    if (!Strings.isNullOrEmpty(suggestion)) {
      buf.append("\nSuggestion: ").append(suggestion);
    }
    return buf.toString();
  }

  @JsonProperty
  @JsonInclude(JsonInclude.Include.NON_EMPTY)
  public Map<String, String> getContext()
  {
    return context;
  }

  public ErrorResponse toErrorResponse()
  {
    return new ErrorResponse(
        category().userText(),
        message(),
        errorClass(),
        host,
        context
    );
  }
}
