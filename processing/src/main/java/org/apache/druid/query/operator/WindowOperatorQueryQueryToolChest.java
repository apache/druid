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

package org.apache.druid.query.operator;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.error.DruidException;
import org.apache.druid.frame.allocation.MemoryAllocatorFactory;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.query.DefaultQueryMetrics;
import org.apache.druid.query.FrameSignaturePair;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.ResultSerializationMode;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.column.Column;
import org.apache.druid.query.rowsandcols.column.ColumnAccessor;
import org.apache.druid.query.rowsandcols.column.NullColumn;
import org.apache.druid.query.rowsandcols.semantic.FrameMaker;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class WindowOperatorQueryQueryToolChest extends QueryToolChest<RowsAndColumns, WindowOperatorQuery>
{

  @Override
  @SuppressWarnings("unchecked")
  public QueryRunner<RowsAndColumns> mergeResults(QueryRunner<RowsAndColumns> runner)
  {
    return new RowsAndColumnsSerializingQueryRunner(
        (queryPlus, responseContext) -> {
          final WindowOperatorQuery query = (WindowOperatorQuery) queryPlus.getQuery();
          final List<OperatorFactory> opFactories = query.getOperators();
          if (opFactories.isEmpty()) {
            return runner.run(queryPlus, responseContext);
          }

          Supplier<Operator> opSupplier = () -> {
            Operator retVal = new SequenceOperator(
                runner.run(
                    queryPlus.withQuery(query.withOperators(new ArrayList<>())),
                    responseContext
                )
            );
            for (OperatorFactory operatorFactory : opFactories) {
              retVal = operatorFactory.wrap(retVal);
            }
            return retVal;
          };

          return new OperatorSequence(opSupplier);
        }
    );
  }

  @Override
  public QueryMetrics<? super WindowOperatorQuery> makeMetrics(WindowOperatorQuery query)
  {
    return new DefaultQueryMetrics<>();
  }

  @Override
  public Function<RowsAndColumns, RowsAndColumns> makePreComputeManipulatorFn(
      WindowOperatorQuery query,
      MetricManipulationFn fn
  )
  {
    return Functions.identity();
  }

  @Override
  public TypeReference<RowsAndColumns> getResultTypeReference()
  {
    return new TypeReference<>() {};
  }

  @Override
  public RowSignature resultArraySignature(WindowOperatorQuery query)
  {
    return query.getRowSignature();
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Sequence<Object[]> resultsAsArrays(
      WindowOperatorQuery query,
      Sequence<RowsAndColumns> resultSequence
  )
  {
    // Dark magic; see RowsAndColumnsSerializingQueryRunner.
    return (Sequence) resultSequence;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Optional<Sequence<FrameSignaturePair>> resultsAsFrames(
      WindowOperatorQuery query,
      Sequence<RowsAndColumns> resultSequence,
      MemoryAllocatorFactory memoryAllocatorFactory,
      boolean useNestedForUnknownTypes
  )
  {
    // see RowsAndColumnsSerializingQueryRunner
    return Optional.of((Sequence) resultSequence);
  }

  /**
   * This class exists to serialize the RowsAndColumns that are used in this query and make it the return Sequence
   * actually be a Sequence of rows or frames, as the query requires.
   * This is relatively broken in a number of regards, the most obvious of which is that it is going to run counter to the stated class on the Generic of the QueryToolChest.
   * That is, the code makes it look like you are getting a Sequence of RowsAndColumns, but, by using this, the query will
   * actually ultimately produce a Sequence of Object[] or Frames.  This works because of type Erasure in Java (it's all Object
   * at the end of the day).
   * <p>
   * While it might seem like this will break all sorts of things, the Generic type is actually there more as a type
   * "hint" to make the writing of the ToolChest and Factory and stuff a bit more simple.  Any caller of this cannot
   * truly depend on the type anyway other than to just throw it across the wire, so this should just magically work
   * even though it looks like it shouldn't even compile.
   * <p>
   * Not our proudest moment, but we use the tools available to us.
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  private static class RowsAndColumnsSerializingQueryRunner implements QueryRunner
  {

    private final QueryRunner<RowsAndColumns> baseQueryRunner;

    private RowsAndColumnsSerializingQueryRunner(
        QueryRunner<RowsAndColumns> baseQueryRunner
    )
    {
      this.baseQueryRunner = baseQueryRunner;
    }

    @Override
    public Sequence run(
        QueryPlus queryPlus,
        ResponseContext responseContext
    )
    {
      // We only want to do this operation once at the very, very top of the execution tree.  So we check and set
      // a context parameter so that if this merge code runs anywhere else, it will skip this part.
      final WindowOperatorQuery query = (WindowOperatorQuery) queryPlus.getQuery();
      if (query.context().getBoolean("unravel", true)) {
        final Sequence<RowsAndColumns> baseSequence = baseQueryRunner.run(
            queryPlus.withQuery(query.withOverriddenContext(ImmutableMap.of("unravel", false))),
            responseContext
        );
        final ResultSerializationMode serializationMode = query.context().getEnum(
            ResultSerializationMode.CTX_SERIALIZATION_PARAMETER,
            ResultSerializationMode.class,
            ResultSerializationMode.ROWS
        );
        switch (serializationMode) {
          case ROWS:
            return asRows(baseSequence, query);
          case FRAMES:
            return asFrames(baseSequence);
          default:
            throw DruidException.defensive("Serialization mode[%s] not supported", serializationMode);
        }
      }

      return baseQueryRunner.run(queryPlus, responseContext);
    }

    /**
     * Translates Sequence of RACs to a Sequence of Object[]
     */
    private static Sequence asRows(final Sequence<RowsAndColumns> baseSequence, final WindowOperatorQuery query)
    {
      final RowSignature rowSignature = query.getRowSignature();
      return baseSequence.flatMap(
          rac -> {
            List<Object[]> results = new ArrayList<>(rac.numRows());

            ColumnAccessor[] accessors = new ColumnAccessor[rowSignature.size()];
            int index = 0;
            for (String columnName : rowSignature.getColumnNames()) {
              final Column column = rac.findColumn(columnName);
              if (column == null) {
                final ColumnType columnType = rowSignature
                    .getColumnType(columnName)
                    .orElse(ColumnType.UNKNOWN_COMPLEX);

                accessors[index] = new NullColumn.Accessor(columnType, rac.numRows());
              } else {
                accessors[index] = column.toAccessor();
              }
              ++index;
            }

            for (int i = 0; i < rac.numRows(); ++i) {
              Object[] objArr = new Object[accessors.length];
              for (int j = 0; j < accessors.length; j++) {
                objArr[j] = accessors[j].getObject(i);
              }
              results.add(objArr);
            }

            return Sequences.simple(results);
          }
      );
    }

    /**
     * Translates a sequence of RACs to a Sequence of Frames
     */
    private static Sequence asFrames(final Sequence<RowsAndColumns> baseSequence)
    {
      return baseSequence.map(
          rac -> {
            FrameMaker frameMaker = FrameMaker.fromRAC(rac);
            return new FrameSignaturePair(
                frameMaker.toColumnBasedFrame(),
                frameMaker.computeSignature()
            );
          }
      );
    }
  }
}
