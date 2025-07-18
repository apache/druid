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

package org.apache.druid.msq.querykit;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import org.apache.druid.query.LeafDataSource;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an input number, i.e., a positional index into
 * {@link org.apache.druid.msq.kernel.StageDefinition#getInputSpecs()}.
 * <p>
 * Used by
 * <ul>
 *   <li>{@link DataSourcePlan}, to note which inputs correspond to which datasources in the query being planned.
 *   <li>{@link BroadcastJoinSegmentMapFnProcessor} to associate broadcast inputs with the correct datasources in a
 * join tree.
 */
@JsonTypeName("inputNumber")
public class InputNumberDataSource extends LeafDataSource
{
  private final int inputNumber;

  @JsonCreator
  public InputNumberDataSource(@JsonProperty("inputNumber") int inputNumber)
  {
    this.inputNumber = inputNumber;
  }

  @Override
  public Set<String> getTableNames()
  {
    return Collections.emptySet();
  }

  @Override
  public boolean isCacheable(boolean isBroker)
  {
    return false;
  }

  @Override
  public boolean isGlobal()
  {
    return false;
  }

  @Override
  public boolean isProcessable()
  {
    // InputNumberDataSource represents InputSpecs, which are scannable via Segment adapters.
    return true;
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
  }

  @JsonProperty
  public int getInputNumber()
  {
    return inputNumber;
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
    InputNumberDataSource that = (InputNumberDataSource) o;
    return inputNumber == that.inputNumber;
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputNumber);
  }

  @Override
  public String toString()
  {
    return "InputNumberDataSource{" +
           "inputNumber=" + inputNumber +
           '}';
  }
}
