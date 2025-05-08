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

package org.apache.druid.msq.indexing.error;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.error.DruidException;

import java.util.Objects;

@JsonTypeName(CanceledFault.CODE)
public class CanceledFault extends BaseMSQFault
{
  public static final String CODE = "Canceled";
  private final Reason reason;

  @JsonCreator
  CanceledFault(@JsonProperty("reason") Reason reason)
  {
    super(CODE, "Query canceled due to [%s].", reason);
    this.reason = reason;
  }

  public static CanceledFault shutdown()
  {
    return new CanceledFault(Reason.TASK_SHUTDOWN);
  }

  public static CanceledFault timeout()
  {
    return new CanceledFault(Reason.QUERY_TIMEOUT);
  }

  @JsonProperty
  public Reason getReason()
  {
    return reason;
  }

  @Override
  public DruidException toDruidException()
  {
    return DruidException.forPersona(DruidException.Persona.USER)
                         .ofCategory(DruidException.Category.CANCELED)
                         .withErrorCode(getErrorCode())
                         .build(MSQFaultUtils.generateMessageWithErrorCode(this));
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    CanceledFault that = (CanceledFault) o;
    return reason == that.reason;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), reason);
  }

  /**
   * Enum denoting the reason for query cancelation.
   */
  public enum Reason
  {
    /**
     * Query was shutdown due to the task shutting down, either due to the task being canceled by the user, or shutdown
     * by the overlord.
     */
    TASK_SHUTDOWN("User cancelation or task shutdown"),
    /**
     * Query was shutdown due to exceeding the configured query timeout.
     */
    QUERY_TIMEOUT("Configured query timeout");

    private final String reason;

    Reason(String reason)
    {
      this.reason = reason;
    }

    @Override
    public String toString()
    {
      return reason;
    }
  }
}
