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

import it.unimi.dsi.fastutil.ints.IntList;
import org.apache.druid.error.DruidException;
import org.apache.druid.query.DataSource;
import org.apache.druid.query.Query;
import org.apache.druid.query.planning.ExecutionVertex;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * One base input of a leaf stage's query, along with the {@link ExecutionVertex} that reads it. A query that reads a
 * union has one per arm, since each arm reads its own input; any other query has just one.
 */
public record BaseInputVertex(int inputNumber, ExecutionVertex vertex)
{
  /**
   * Returns the base inputs of a query, flattening a union once if it occurs, in the order of
   * {@link ExecutionVertex#splitUnion()}.
   */
  public static List<BaseInputVertex> createAll(final Query<?> query)
  {
    final List<BaseInputVertex> retVal = new ArrayList<>();

    for (final ExecutionVertex subVertex : ExecutionVertex.of(query).splitUnion()) {
      final Integer inputNumber = getInputNumber(subVertex.getBaseDataSource());
      if (inputNumber == null) {
        throw DruidException.defensive(
            "Base dataSource[%s] does not have an associated inputNumber",
            subVertex.getBaseDataSource()
        );
      }
      retVal.add(new BaseInputVertex(inputNumber, subVertex));
    }

    // Sanity check: ensure no duplicate input numbers.
    if (retVal.size() != retVal.stream().map(BaseInputVertex::inputNumber).collect(Collectors.toSet()).size()) {
      throw DruidException.defensive(
          "Duplicate inputNumbers[%s]",
          retVal.stream().map(BaseInputVertex::inputNumber).toList()
      );
    }

    return retVal;
  }

  /**
   * Returns the query with its base datasource replaced by the stage's sole base input, if the base is a leaf that
   * is not recognized by {@link #getInputNumber(DataSource)}. This is not necessary for newer controllers, since
   * they always use numbered datasources for leafs. It is necessary for backwards compatibility with
   * older controllers.
   *
   * @param query            the stage's query
   * @param baseInputNumbers the stage's base input numbers, i.e. those that are not broadcast
   */
  public static Query<?> withNumberedBase(final Query<?> query, final IntList baseInputNumbers)
  {
    final ExecutionVertex vertex = ExecutionVertex.of(query);
    final DataSource baseDataSource = vertex.getBaseDataSource();
    if (baseInputNumbers.size() == 1
        && baseDataSource.getChildren().isEmpty()
        && getInputNumber(baseDataSource) == null) {
      // Older controllers send a sole base input as its original datasource, such as external, not an input number.
      return vertex.buildQueryWithBaseDataSource(new InputNumberDataSource(baseInputNumbers.getInt(0)));
    } else {
      return query;
    }
  }

  /**
   * Returns the input number associated with a base datasource, or null if it names none.
   */
  @Nullable
  private static Integer getInputNumber(final DataSource baseDataSource)
  {
    if (baseDataSource instanceof InputNumberDataSource) {
      return ((InputNumberDataSource) baseDataSource).getInputNumber();
    } else if (baseDataSource instanceof RestrictedInputNumberDataSource) {
      return ((RestrictedInputNumberDataSource) baseDataSource).getInputNumber();
    } else {
      return null;
    }
  }
}
