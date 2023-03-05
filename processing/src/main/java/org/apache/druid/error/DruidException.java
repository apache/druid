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

import org.apache.commons.text.StringSubstitutor;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

/**
 * Represents an error condition exposed to the user and/or operator of Druid.
 * Every error consists of:
 * <ul>
 * <li>An error code.</li>
 * <li>A set of zero or more parameters.</li>
 * <li>A default error message "template".</li>
 * </ul>
 * <p>
 * The error code is a unique identifier for each and every distinct
 * kind of error. Codes <i>should</i> follow the pattern of
 * @{code <Major-group>-<minor-group>-<error>} such as
 * @{code SQL-VALIDATION-UNKNOWN_COLUMN}.
 * <p>
 * The message template is a user-visible explanation for the error.
 * The message is a template because it contains named placeholders
 * to fill in with parameters:<br>
 * "Line ${line}, Column ${column}: Column [${name}] not found"<br>
 * <p>
 * The parameters are the values to fill in the placeholders in the
 * template. Each subclass defines the parameters for that error, along
 * with the required mapping to placeholders.
 * <p>
 * With this system, extensions can translate the messages to the needs
 * of a specific system. For example, if system generates SQL, then telling
 * the user the line number of the error is just confusing. In that system,
 * the error could be translated to:<br>
 * "Field '${name}' is not defined. Check the field list."<br>
 * <p>
 * Exceptions are mutable and must not be modified by two threads concurrently.
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
 *      e.setFileName(theFile.getName());
 *      throw e;
 *   }
 * }
 * </code></pre>
 * <p>
 * Exceptions are not serializable. Instead, exceptions are translated
 * to some other form when sent over the wire.
 */
@NotThreadSafe
public abstract class DruidException extends RuntimeException
{
  public static final String SIMPLE_MESSAGE = "${message}";
  public static final String MESSAGE_KEY = "message";

  private final String code;
  private final String message;
  protected final Map<String, String> values = new LinkedHashMap<>();
  protected String legacyCode;
  protected String legacyClass;

  public DruidException(
      final String code,
      final String message
  )
  {
    this(null, code, message);
  }

  public DruidException(
      final Throwable cause,
      final String code,
      final String message
  )
  {
    super(code, cause);
    this.code = code;
    this.message = message;
  }

  public DruidException withValue(String key, Object value)
  {
    values.put(key, Objects.toString(value));
    return this;
  }

  public DruidException withValues(Map<String, String> values)
  {
    this.values.putAll(values);
    return this;
  }

  /**
   * The error code is a summary of the error returned to the user. Multiple errors
   * map to the same code: the code is more like a category of errors. Error codes
   * must be backward compatible, even if the prior "codes" are awkward.
   */
  public String errorCode()
  {
    return code;
  }

  public String message()
  {
    return message;
  }

  public Map<String, String> values()
  {
    return values;
  }

  // Used primarily when logging an error.
  @Override
  public String getMessage()
  {
    if (values.isEmpty()) {
      return code;
    }
    List<String> entries = new ArrayList<>();
    for (Map.Entry<String, String> entry : values.entrySet()) {
      entries.add(entry.getKey() + "=[" + entry.getValue() + "]");
    }
    return code + ": " + String.join(", ", entries);
  }

  // For debugging.
  @Override
  public String toString()
  {
    return format(message);
  }

  public String format(String template)
  {
    StringSubstitutor sub = new StringSubstitutor(values);
    return sub.replace(template);
  }

  public String format(Properties catalog)
  {
    String template = catalog.getProperty(code);
    if (template == null) {
      return toString();
    } else {
      return format(template);
    }
  }

  public ErrorResponse toErrorResponse(Properties catalog)
  {
    return new ErrorResponse(
        code,
        format(catalog),
        legacyClass,
        null
    );
  }

  public abstract ErrorAudience audience();
  public abstract int httpStatus();

  public MetricCategory metricCategory()
  {
    return MetricCategory.FAILED;
  }

  public String getErrorCode()
  {
    return legacyCode;
  }
}
