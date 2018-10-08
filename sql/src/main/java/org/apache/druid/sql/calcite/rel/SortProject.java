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
package org.apache.druid.sql.calcite.rel;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.aggregation.PostAggregator;
import org.apache.druid.sql.calcite.table.RowSignature;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class SortProject
{
  private final RowSignature inputRowSignature;
  private final List<PostAggregator> postAggregators;
  private final RowSignature outputRowSignature;

  SortProject(
      RowSignature inputRowSignature,
      List<PostAggregator> postAggregators,
      RowSignature outputRowSignature
  )
  {
    this.inputRowSignature = Preconditions.checkNotNull(inputRowSignature, "inputRowSignature");
    this.postAggregators = Preconditions.checkNotNull(postAggregators, "postAggregators");
    this.outputRowSignature = Preconditions.checkNotNull(outputRowSignature, "outputRowSignature");

    final Set<String> inputColumnNames = new HashSet<>(inputRowSignature.getRowOrder());
    final Set<String> postAggregatorNames = postAggregators.stream()
                                                           .map(PostAggregator::getName)
                                                           .collect(Collectors.toSet());

    // Verify no collisions between inputs and outputs.
    for (String postAggregatorName : postAggregatorNames) {
      if (inputColumnNames.contains(postAggregatorName)) {
        throw new ISE("Duplicate field name: %s", postAggregatorName);
      }
    }

    // Verify that items in the output signature exist.
    outputRowSignature.getRowOrder().forEach(field -> {
      if (!inputColumnNames.contains(field) && !postAggregatorNames.contains(field)) {
        throw new ISE("Missing field in rowOrder: %s", field);
      }
    });
  }

  public List<PostAggregator> getPostAggregators()
  {
    return postAggregators;
  }

  public RowSignature getOutputRowSignature()
  {
    return outputRowSignature;
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
    SortProject sortProject = (SortProject) o;
    return Objects.equals(inputRowSignature, sortProject.inputRowSignature) &&
           Objects.equals(postAggregators, sortProject.postAggregators) &&
           Objects.equals(outputRowSignature, sortProject.outputRowSignature);
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(inputRowSignature, postAggregators, outputRowSignature);
  }

  @Override
  public String toString()
  {
    return "SortProject{" +
           "inputRowSignature=" + inputRowSignature +
           ", postAggregators=" + postAggregators +
           ", outputRowSignature=" + outputRowSignature +
           '}';
  }
}
