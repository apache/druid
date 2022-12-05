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
import org.apache.druid.msq.guice.MSQDurableStorageModule;
import org.apache.druid.msq.util.MultiStageQueryContext;

import java.util.Objects;

@JsonTypeName(DurableStorageConfigurationFault.CODE)
public class DurableStorageConfigurationFault extends BaseMSQFault
{
  static final String CODE = "DurableStorageConfiguration";

  private final String errorMessage;

  @JsonCreator
  public DurableStorageConfigurationFault(@JsonProperty("message") final String errorMessage)
  {
    super(
        CODE,
        "Durable storage mode can only be enabled when %s is set to true and "
        + "the connector is configured correctly. "
        + "Check the documentation on how to enable durable storage mode. "
        + "If you want to still query without durable storage mode, set %s to false in the query context. Got error %s",
        MSQDurableStorageModule.MSQ_INTERMEDIATE_STORAGE_ENABLED,
        MultiStageQueryContext.CTX_ENABLE_DURABLE_SHUFFLE_STORAGE,
        errorMessage
    );
    this.errorMessage = errorMessage;
  }

  @JsonProperty
  public String getMessage()
  {
    return errorMessage;
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
    DurableStorageConfigurationFault that = (DurableStorageConfigurationFault) o;
    return Objects.equals(errorMessage, that.errorMessage);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(super.hashCode(), errorMessage);
  }
}
