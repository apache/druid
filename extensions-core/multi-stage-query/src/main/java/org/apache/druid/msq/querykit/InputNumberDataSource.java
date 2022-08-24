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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.query.DataSource;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@JsonTypeName("inputNumber")
public class InputNumberDataSource implements DataSource
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
  public List<DataSource> getChildren()
  {
    return Collections.emptyList();
  }

  @Override
  public DataSource withChildren(final List<DataSource> children)
  {
    if (!children.isEmpty()) {
      throw new IAE("Cannot accept children");
    }

    return this;
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
  public boolean isConcrete()
  {
    return false;
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
