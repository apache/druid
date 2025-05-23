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
import com.google.common.base.Preconditions;
import org.apache.druid.query.LeafDataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.policy.Policy;
import org.apache.druid.segment.RestrictedSegment;
import org.apache.druid.segment.SegmentMapFunction;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an input number, i.e., a positional index into
 * {@link org.apache.druid.msq.kernel.StageDefinition#getInputSpecs()}, with policy restriction.
 * <p>
 * Used by
 * <ul>
 *   <li>{@link DataSourcePlan}, to note which inputs correspond to which datasources in the query being planned.
 *   <li>{@link BroadcastJoinSegmentMapFnProcessor} to associate broadcast inputs with the correct datasources in a
 * join tree.
 */
@JsonTypeName("restrictedInputNumber")
public class RestrictedInputNumberDataSource extends LeafDataSource
{
  private final int inputNumber;
  private final Policy policy;

  @JsonCreator
  public RestrictedInputNumberDataSource(
      @JsonProperty("inputNumber") int inputNumber,
      @JsonProperty("policy") Policy policy
  )
  {
    this.inputNumber = inputNumber;
    this.policy = Preconditions.checkNotNull(policy, "Policy can't be null");
  }

  @JsonProperty
  public int getInputNumber()
  {
    return inputNumber;
  }

  @JsonProperty
  public Policy getPolicy()
  {
    return policy;
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
    return true;
  }

  @Override
  public SegmentMapFunction createSegmentMapFunction(Query query)
  {
    return SegmentMapFunction.IDENTITY.thenMap(segment -> new RestrictedSegment(segment, policy));
  }

  @Override
  public byte[] getCacheKey()
  {
    return null;
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
    RestrictedInputNumberDataSource that = (RestrictedInputNumberDataSource) o;
    return inputNumber == that.inputNumber && policy.equals(that.policy);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputNumber, policy);
  }

  @Override
  public String toString()
  {
    return "RestrictedInputNumberDataSource{" +
           "inputNumber=" + inputNumber +
           ", policy=" + policy + "}";

  }
}
