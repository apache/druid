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

package org.apache.druid.msq.exec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.msq.indexing.MSQControllerTask;
import org.apache.druid.msq.indexing.MSQSpec;
import org.apache.druid.msq.indexing.MSQTuningConfig;
import org.apache.druid.msq.indexing.destination.DataSourceMSQDestination;
import org.apache.druid.msq.indexing.destination.ExportMSQDestination;
import org.apache.druid.msq.indexing.destination.MSQDestination;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.QueryNotSupportedFault;
import org.apache.druid.msq.input.InputSpecs;
import org.apache.druid.msq.input.stage.StageInputSpec;
import org.apache.druid.msq.kernel.QueryDefinition;
import org.apache.druid.msq.kernel.QueryDefinitionBuilder;
import org.apache.druid.msq.kernel.StageDefinition;
import org.apache.druid.msq.querykit.MultiQueryKit;
import org.apache.druid.msq.querykit.QueryKit;
import org.apache.druid.msq.querykit.QueryKitSpec;
import org.apache.druid.msq.querykit.QueryKitUtils;
import org.apache.druid.msq.querykit.ShuffleSpecFactory;
import org.apache.druid.msq.querykit.WindowOperatorQueryKit;
import org.apache.druid.msq.querykit.groupby.GroupByQueryKit;
import org.apache.druid.msq.querykit.results.ExportResultsStageProcessor;
import org.apache.druid.msq.querykit.results.QueryResultStageProcessor;
import org.apache.druid.msq.querykit.scan.ScanQueryKit;
import org.apache.druid.msq.util.MSQTaskQueryMakerUtils;
import org.apache.druid.msq.util.MultiStageQueryContext;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryContext;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.operator.WindowOperatorQuery;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.sql.calcite.planner.ColumnMappings;
import org.apache.druid.sql.http.ResultFormat;
import org.apache.druid.storage.ExportStorageProvider;

import java.util.Map;

public class QueryKitBasedMSQPlanner
{
  private final MSQSpec querySpec;
  private final ResultsContext resultsContext;
  private final QueryKitSpec queryKitSpec;
  private final ObjectMapper jsonMapper;
  private final MSQTuningConfig tuningConfig;
  private final ColumnMappings columnMappings;
  private final MSQDestination destination;
  private final QueryContext queryContext;
  private final Query<?> query;


  public QueryKitBasedMSQPlanner(
      MSQSpec querySpec,
      ResultsContext resultsContext,
      Query<?> query,
      ObjectMapper jsonMapper,
      QueryKitSpec queryKitSpec)
  {
    this.querySpec = querySpec;
    this.jsonMapper = jsonMapper;
    this.tuningConfig = querySpec.getTuningConfig();
    this.columnMappings = querySpec.getColumnMappings();
    this.destination = querySpec.getDestination();
    this.queryContext = querySpec.getContext();
    this.query = query;
    this.resultsContext = resultsContext;
    this.queryKitSpec = queryKitSpec;
  }

  @SuppressWarnings("rawtypes")
  public static QueryKit<Query<?>> makeQueryControllerToolKit(QueryContext queryContext, ObjectMapper jsonMapper)
  {
    final Map<Class<? extends Query>, QueryKit> kitMap =
        ImmutableMap.<Class<? extends Query>, QueryKit>builder()
                    .put(ScanQuery.class, new ScanQueryKit(jsonMapper))
                    .put(GroupByQuery.class, new GroupByQueryKit(jsonMapper))
                    .put(
                        WindowOperatorQuery.class,
                        new WindowOperatorQueryKit(
                            jsonMapper,
                            MultiStageQueryContext.isWindowFunctionOperatorTransformationEnabled(queryContext)
                        )
                    )
                    .build();

    return new MultiQueryKit(kitMap);
  }

  @SuppressWarnings("unchecked")
  public QueryDefinition makeQueryDefinition()
  {
    final Query<?> queryToPlan;

    if (MSQControllerTask.isIngestion(destination)) {
      if (!columnMappings.hasUniqueOutputColumnNames()) {
        // We do not expect to hit this case in production, because the SQL validator checks that column names
        // are unique for INSERT and REPLACE statements (i.e. anything where MSQControllerTask.isIngestion would
        // be true). This check is here as defensive programming.
        throw new ISE("Column names are not unique: [%s]", columnMappings.getOutputColumnNames());
      }

      MSQTaskQueryMakerUtils.validateRealtimeReindex(queryContext, destination, query);

      if (columnMappings.hasOutputColumn(ColumnHolder.TIME_COLUMN_NAME)) {
        // We know there's a single time column, because we've checked columnMappings.hasUniqueOutputColumnNames().
        final int timeColumn = columnMappings.getOutputColumnsByName(ColumnHolder.TIME_COLUMN_NAME).getInt(0);
        queryToPlan = query.withOverriddenContext(
            ImmutableMap.of(
                QueryKitUtils.CTX_TIME_COLUMN_NAME,
                columnMappings.getQueryColumnName(timeColumn)
            )
        );
      } else {
        queryToPlan = query;
      }
    } else {
      queryToPlan = query;
    }

    return makeQueryDefinitionInternal(queryToPlan).withOverriddenContext(queryToPlan.getContext());
  }

  private QueryDefinition makeQueryDefinitionInternal(final Query<?> queryToPlan)
  {
    final QueryDefinition queryDef;

    try {
      queryDef = queryKitSpec.getQueryKit().makeQueryDefinition(
          queryKitSpec,
          queryToPlan,
          makeResultShuffleSpecFacory(),
          0
      );
    }
    catch (MSQException e) {
      // If the toolkit throws a MSQFault, don't wrap it in a more generic QueryNotSupportedFault
      throw e;
    }
    catch (Exception e) {
      throw new MSQException(e, QueryNotSupportedFault.INSTANCE);
    }

    if (MSQControllerTask.isIngestion(destination)) {
      // Find the stage that provides shuffled input to the final segment-generation stage.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();

      while (!finalShuffleStageDef.doesShuffle()
             && InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()).size() == 1) {
        finalShuffleStageDef = queryDef.getStageDefinition(
            Iterables.getOnlyElement(InputSpecs.getStageNumbers(finalShuffleStageDef.getInputSpecs()))
        );
      }

      if (!finalShuffleStageDef.doesShuffle()) {
        finalShuffleStageDef = null;
      }

      // Add all query stages.
      // Set shuffleCheckHasMultipleValues on the stage that serves as input to the final segment-generation stage.
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());

      for (final StageDefinition stageDef : queryDef.getStageDefinitions()) {
        if (stageDef.equals(finalShuffleStageDef)) {
          builder.add(StageDefinition.builder(stageDef).shuffleCheckHasMultipleValues(true));
        } else {
          builder.add(StageDefinition.builder(stageDef));
        }
      }

      final DataSourceMSQDestination destination1 = (DataSourceMSQDestination) destination;
      return builder.add(
                        destination1.getTerminalStageSpec()
                                   .constructFinalStage(
                                       queryDef,
                                       querySpec,
                                       jsonMapper,
                                       query
                                   )
                    )
                    .build();
    } else if (MSQControllerTask.writeFinalResultsToTaskReport(destination)) {
      return queryDef;
    } else if (MSQControllerTask.writeFinalStageResultsToDurableStorage(destination)) {

      // attaching new query results stage if the final stage does sort during shuffle so that results are ordered.
      StageDefinition finalShuffleStageDef = queryDef.getFinalStageDefinition();
      if (finalShuffleStageDef.doesSortDuringShuffle()) {
        final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
        builder.addAll(queryDef);
        builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                   .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                   .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                   .signature(finalShuffleStageDef.getSignature())
                                   .shuffleSpec(null)
                                   .processor(new QueryResultStageProcessor())
        );
        return builder.build();
      } else {
        return queryDef;
      }
    } else if (MSQControllerTask.isExport(destination)) {
      final ExportMSQDestination exportMSQDestination = (ExportMSQDestination) destination;
      final ExportStorageProvider exportStorageProvider = exportMSQDestination.getExportStorageProvider();

      final ResultFormat resultFormat = exportMSQDestination.getResultFormat();
      final QueryDefinitionBuilder builder = QueryDefinition.builder(queryKitSpec.getQueryId());
      builder.addAll(queryDef);
      builder.add(StageDefinition.builder(queryDef.getNextStageNumber())
                                 .inputs(new StageInputSpec(queryDef.getFinalStageDefinition().getStageNumber()))
                                 .maxWorkerCount(tuningConfig.getMaxNumWorkers())
                                 .signature(queryDef.getFinalStageDefinition().getSignature())
                                 .shuffleSpec(null)
                                 .processor(new ExportResultsStageProcessor(
                                     queryKitSpec.getQueryId(),
                                     exportStorageProvider,
                                     resultFormat,
                                     columnMappings,
                                     resultsContext
                                 ))
      );
      return builder.build();
    } else {
      throw new ISE("Unsupported destination [%s]", destination);
    }
  }

  private ShuffleSpecFactory makeResultShuffleSpecFacory()
  {
    if (MSQControllerTask.isIngestion(destination)) {
      return destination.getShuffleSpecFactory(tuningConfig.getRowsPerSegment());
    } else {
      return destination.getShuffleSpecFactory(MultiStageQueryContext.getRowsPerPage(query.context()));
    }
  }
}
