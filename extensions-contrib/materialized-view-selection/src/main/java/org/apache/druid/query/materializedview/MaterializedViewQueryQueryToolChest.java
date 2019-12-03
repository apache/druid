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

package org.apache.druid.query.materializedview;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.base.Function;
import com.google.inject.Inject;
import org.apache.druid.data.input.Row;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.QueryToolChest;
import org.apache.druid.query.QueryToolChestWarehouse;
import org.apache.druid.query.aggregation.MetricManipulationFn;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.ResultRow;

import java.io.IOException;
import java.util.Comparator;
import java.util.function.BinaryOperator;

public class MaterializedViewQueryQueryToolChest extends QueryToolChest 
{
  private final QueryToolChestWarehouse warehouse;
  private DataSourceOptimizer optimizer;

  @Inject
  public MaterializedViewQueryQueryToolChest(
      QueryToolChestWarehouse warehouse
  )
  {
    this.warehouse = warehouse;
  }
  
  @Override
  public QueryRunner mergeResults(QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        return warehouse.getToolChest(realQuery).mergeResults(runner).run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }

  @Override
  public BinaryOperator createMergeFn(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createMergeFn(realQuery);
  }

  @Override
  public Comparator createResultComparator(Query query)
  {
    final Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).createResultComparator(realQuery);
  }

  @Override
  public QueryMetrics makeMetrics(Query query) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makeMetrics(realQuery);
  }
  
  @Override
  public Function makePreComputeManipulatorFn(Query query, MetricManipulationFn fn) 
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePreComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public Function makePostComputeManipulatorFn(Query query, MetricManipulationFn fn)
  {
    Query realQuery = getRealQuery(query);
    return warehouse.getToolChest(realQuery).makePostComputeManipulatorFn(realQuery, fn);
  }

  @Override
  public TypeReference getResultTypeReference()
  {
    return null;
  }

  @Override
  public QueryRunner preMergeQueryDecoration(final QueryRunner runner)
  {
    return new QueryRunner() {
      @Override
      public Sequence run(QueryPlus queryPlus, ResponseContext responseContext)
      {
        Query realQuery = getRealQuery(queryPlus.getQuery());
        QueryToolChest realQueryToolChest = warehouse.getToolChest(realQuery);
        QueryRunner realQueryRunner = realQueryToolChest.preMergeQueryDecoration(
            new MaterializedViewQueryRunner(runner, optimizer)
        );
        return realQueryRunner.run(queryPlus.withQuery(realQuery), responseContext);
      }
    };
  }
  
  public Query getRealQuery(Query query)
  {
    if (query instanceof MaterializedViewQuery) {
      optimizer = ((MaterializedViewQuery) query).getOptimizer();
      return ((MaterializedViewQuery) query).getQuery();
    }
    return query;
  }

  @Override
  public ObjectMapper decorateObjectMapper(final ObjectMapper objectMapper, final Query query)
  {
    if (!(getRealQuery(query) instanceof GroupByQuery)) {
      return objectMapper;
    }
    final boolean resultAsArray = query.getContextBoolean(GroupByQueryConfig.CTX_KEY_ARRAY_RESULT_ROWS, false);

    // Serializer that writes array- or map-based rows as appropriate, based on the "resultAsArray" setting.
    final JsonSerializer<ResultRow> serializer = new JsonSerializer<ResultRow>()
    {
      @Override
      public void serialize(
              final ResultRow resultRow,
              final JsonGenerator jg,
              final SerializerProvider serializers
      ) throws IOException
      {
        if (resultAsArray) {
          jg.writeObject(resultRow.getArray());
        } else {
          jg.writeObject(resultRow.toMapBasedRow((GroupByQuery) getRealQuery(query)));
        }
      }
    };

    // Deserializer that can deserialize either array- or map-based rows.
    final JsonDeserializer<ResultRow> deserializer = new JsonDeserializer<ResultRow>()
    {
      @Override
      public ResultRow deserialize(final JsonParser jp, final DeserializationContext ctxt) throws IOException
      {
        if (jp.isExpectedStartObjectToken()) {
          final Row row = jp.readValueAs(Row.class);
          return ResultRow.fromLegacyRow(row, (GroupByQuery) getRealQuery(query));
        } else {
          return ResultRow.of(jp.readValueAs(Object[].class));
        }
      }
    };

    class MaterializedViewResultRowModule extends SimpleModule
    {
      private MaterializedViewResultRowModule()
      {
        addSerializer(ResultRow.class, serializer);
        addDeserializer(ResultRow.class, deserializer);
      }
    }

    final ObjectMapper newObjectMapper = objectMapper.copy();
    newObjectMapper.registerModule(new MaterializedViewResultRowModule());
    return newObjectMapper;
  }
}
