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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.druid.query.QueryException;

import javax.annotation.Nullable;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A Response Object that represents an error to be returned over the wire.  This object carries legacy bits to
 * deal with compatibility issues of converging the error responses from {@link QueryException}
 * with the intended going-forward error responses from {@link DruidException}
 * <p>
 * The intent is that eventually {@link QueryException} is completely subsumed by
 * {@link DruidException} in which case the legacy bits of this class can hopefully also be removed.
 * <p>
 * The intended long-term schema of output is an object that looks like
 * <p>
 * {
 * "errorCode": `a code string`,
 * "persona": USER | ADMIN | OPERATOR | DEVELOPER
 * "category": DEFENSIVE | INVALID_INPUT | UNAUTHORIZED | CAPACITY_EXCEEDED | CANCELED | RUNTIME_FAILURE | TIMEOUT | UNSUPPORTED | UNCATEGORIZED
 * "errorMessage": `a message for the intended audience`
 * "context": `a map of extra context values that might be helpful`
 * }
 * <p>
 * In the interim, there are extra fields that also end up included so that the wire-schema can also be interpretted
 * and handled by clients that are built assuming they are looking at QueryExceptions.  These extra fields are
 * <p>
 * {
 * "error": `an error code from QueryException` | "druidException"
 * "errorClass": `the error class, as used by QueryException`
 * "host": `the host that the exception occurred on, as used by QueryException`
 * }
 * <p>
 * These 3 top-level fields are deprecated and will eventually disappear from API responses.  The values can, instead,
 * be pulled from the context object of an "legacyQueryException" errorCode object.  The field names in the context
 * object map as follows
 * * "error" -> "legacyErrorCode"
 * * "errorClass" -> "errorClass"
 * * "host" -> "host"
 */
public class ErrorResponse
{
  @JsonCreator
  public static ErrorResponse fromMap(Map<String, Object> map)
  {
    final DruidException.Failure failure;

    final Object legacyErrorType = map.get("error");
    if (!"druidException".equals(legacyErrorType)) {
      // The non "druidException" errorCode field means that we are deserializing a legacy QueryException object rather
      // than deserializing a DruidException.  So, we make a QueryException, map it to a DruidException and build
      // our response from that DruidException.  This allows all code after us to only consider DruidException
      // and helps aid the removal of QueryException.
      failure = new QueryExceptionCompat(
          new QueryException(
              nullOrString(map.get("error")),
              nullOrString(map.get("errorMessage")),
              nullOrString(map.get("errorClass")),
              nullOrString(map.get("host"))
          )
      );
    } else {
      failure = new DruidException.Failure(stringOrFailure(map, "errorCode"))
      {
        @Override
        protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
        {
          final DruidException retVal = bob.forPersona(DruidException.Persona.valueOf(stringOrFailure(map, "persona")))
                                           .ofCategory(DruidException.Category.valueOf(stringOrFailure(
                                               map,
                                               "category"
                                           )))
                                           .build(stringOrFailure(map, "errorMessage"));

          final Object context = map.get("context");
          if (context instanceof Map) {
            //noinspection unchecked
            retVal.withContext((Map<String, String>) context);
          }

          return retVal;
        }
      };
    }
    return new ErrorResponse(DruidException.fromFailure(new DeserializedFailure(failure)));
  }

  private final DruidException underlyingException;

  public ErrorResponse(DruidException underlyingException)
  {
    this.underlyingException = underlyingException;
  }

  @JsonValue
  public Map<String, Object> getAsMap()
  {
    final LinkedHashMap<String, Object> retVal = new LinkedHashMap<>();

    // This if statement is a compatibility layer to help bridge the time while we are introducing the DruidException.
    // In a future release, QueryException should be completely eliminated, at which point we should also be
    // able to eliminate this compatibility layer.
    if (QueryExceptionCompat.ERROR_CODE.equals(underlyingException.getErrorCode())) {
      retVal.put("error", underlyingException.getContextValue("legacyErrorCode"));
      retVal.put("errorClass", underlyingException.getContextValue("errorClass"));
      retVal.put("host", underlyingException.getContextValue("host"));
    } else {
      retVal.put("error", "druidException");
    }

    retVal.put("errorCode", underlyingException.getErrorCode());
    retVal.put("persona", underlyingException.getTargetPersona().toString());
    retVal.put("category", underlyingException.getCategory().toString());
    retVal.put("errorMessage", underlyingException.getMessage());
    retVal.put("context", underlyingException.getContext());

    return retVal;
  }

  public DruidException getUnderlyingException()
  {
    return underlyingException;
  }

  @Nullable
  private static String nullOrString(Object o)
  {
    return o == null ? null : o.toString();
  }

  private static String stringOrFailure(Map<String, Object> map, String key)
  {
    final Object o = map.get(key);
    if (o instanceof String) {
      return (String) o;
    }

    final DruidException problem = DruidException
        .forPersona(DruidException.Persona.DEVELOPER)
        .ofCategory(DruidException.Category.DEFENSIVE)
        .build("Got an error response that had a non-String value [%s] for key [%s]", o, key);

    for (Map.Entry<String, Object> entry : map.entrySet()) {
      final Object value = entry.getValue();
      if (value != null) {
        problem.withContext(entry.getKey(), value.toString());
      }
    }

    throw problem;
  }

  private static class DeserializedFailure extends DruidException.Failure
  {
    private final DruidException.Failure delegate;

    public DeserializedFailure(
        DruidException.Failure delegate
    )
    {
      super(delegate.getErrorCode());
      this.delegate = delegate;
    }

    @Override
    protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
    {
      // By setting wasDeserialized, we get the initial exception built with no stack-trace, we then create a new
      // exception with the exact same values that will contain our current stack-trace and to be relevant inside
      // of the current process.  It's a little bit of a weird dance to create a new exception with the same stuff,
      // it might be nice to have a DelegatingDruidException or something like that which looks like a DruidException
      // but just delegates everything.  That's something that can be explored another day though.
      bob.wasDeserialized();
      final DruidException cause = delegate.makeException(bob);

      return DruidException.fromFailure(
          new DruidException.Failure(cause.getErrorCode())
          {
            @Override
            protected DruidException makeException(DruidException.DruidExceptionBuilder bob)
            {
              return bob.forPersona(cause.getTargetPersona())
                        .ofCategory(cause.getCategory())
                        .build(cause, "%s", cause.getMessage())
                        .withContext(cause.getContext());
            }
          }
      );
    }
  }
}
