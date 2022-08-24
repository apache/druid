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

package org.apache.druid.msq.indexing.report;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.Yielders;
import org.apache.druid.segment.column.RowSignature;

import javax.annotation.Nullable;
import java.util.List;

public class MSQResultsReport
{
  private final RowSignature signature;
  @Nullable
  private final List<String> sqlTypeNames;
  private final Yielder<Object[]> resultYielder;

  public MSQResultsReport(
      final RowSignature signature,
      @Nullable final List<String> sqlTypeNames,
      final Yielder<Object[]> resultYielder
  )
  {
    this.signature = Preconditions.checkNotNull(signature, "signature");
    this.sqlTypeNames = sqlTypeNames;
    this.resultYielder = Preconditions.checkNotNull(resultYielder, "resultYielder");
  }

  /**
   * Method that enables Jackson deserialization.
   */
  @JsonCreator
  static MSQResultsReport fromJson(
      @JsonProperty("signature") final RowSignature signature,
      @JsonProperty("sqlTypeNames") @Nullable final List<String> sqlTypeNames,
      @JsonProperty("results") final List<Object[]> results
  )
  {
    return new MSQResultsReport(signature, sqlTypeNames, Yielders.each(Sequences.simple(results)));
  }

  @JsonProperty("signature")
  public RowSignature getSignature()
  {
    return signature;
  }

  @Nullable
  @JsonProperty("sqlTypeNames")
  @JsonInclude(JsonInclude.Include.NON_NULL)
  public List<String> getSqlTypeNames()
  {
    return sqlTypeNames;
  }

  @JsonProperty("results")
  public Yielder<Object[]> getResultYielder()
  {
    return resultYielder;
  }
}
