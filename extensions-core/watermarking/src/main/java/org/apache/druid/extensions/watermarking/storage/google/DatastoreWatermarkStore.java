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

package org.apache.druid.extensions.watermarking.storage.google;

import com.google.api.services.datastore.v1.Datastore;
import com.google.api.services.datastore.v1.model.BeginTransactionRequest;
import com.google.api.services.datastore.v1.model.BeginTransactionResponse;
import com.google.api.services.datastore.v1.model.CommitRequest;
import com.google.api.services.datastore.v1.model.CommitResponse;
import com.google.api.services.datastore.v1.model.CompositeFilter;
import com.google.api.services.datastore.v1.model.Entity;
import com.google.api.services.datastore.v1.model.EntityResult;
import com.google.api.services.datastore.v1.model.Filter;
import com.google.api.services.datastore.v1.model.Key;
import com.google.api.services.datastore.v1.model.KindExpression;
import com.google.api.services.datastore.v1.model.Mutation;
import com.google.api.services.datastore.v1.model.MutationResult;
import com.google.api.services.datastore.v1.model.PartitionId;
import com.google.api.services.datastore.v1.model.PathElement;
import com.google.api.services.datastore.v1.model.Projection;
import com.google.api.services.datastore.v1.model.PropertyFilter;
import com.google.api.services.datastore.v1.model.PropertyOrder;
import com.google.api.services.datastore.v1.model.PropertyReference;
import com.google.api.services.datastore.v1.model.Query;
import com.google.api.services.datastore.v1.model.QueryResultBatch;
import com.google.api.services.datastore.v1.model.ReadWrite;
import com.google.api.services.datastore.v1.model.RunQueryRequest;
import com.google.api.services.datastore.v1.model.RunQueryResponse;
import com.google.api.services.datastore.v1.model.TransactionOptions;
import com.google.api.services.datastore.v1.model.Value;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.extensions.watermarking.WatermarkKeeperConfig;
import org.apache.druid.extensions.watermarking.storage.WatermarkStore;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.format.ISODateTimeFormat;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public class DatastoreWatermarkStore implements WatermarkStore
{
  private static final Logger log = new Logger(DatastoreWatermarkStore.class);
  // From http://googlecloudplatform.github.io/google-cloud-java/0.20.3/apidocs/com/google/cloud/Timestamp.html
  private static final DateTime TIMESTAMP_MIN = DateTimes.of("0001-01-01T00:00:00Z");
  private static final DateTime TIMESTAMP_MAX = DateTimes.of("9999-12-31T23:59:59Z");

  private static final String KIND = "druidTimelineMetadata";
  private static final String TIMESTAMP_COLUMN = "timestamp";
  private static final String INSERT_TIMESTAMP_COLUMN = "insert_timestamp";
  private static final String TYPE_COLUMN = "type";
  private static final String DATASOURCE = "datasource";

  private final Datastore datastore;
  private final DatastoreWatermarkStoreConfig datastoreConfig;
  private final WatermarkKeeperConfig keeperConfig;
  private final ConcurrentHashMap<String, AtomicLong> maxStore;

  @Inject
  public DatastoreWatermarkStore(
      Datastore datastore,
      DatastoreWatermarkStoreConfig datastoreConfig,
      WatermarkKeeperConfig keeperConfig
  )
  {
    this.datastore = datastore;
    this.datastoreConfig = datastoreConfig;
    this.keeperConfig = keeperConfig;
    this.maxStore = new ConcurrentHashMap<>();
    log.info("Configured Google Cloud Datastore as timeline watermark metadata storage");
  }

  protected static String keyGen(String datasource, String type)
  {
    return StringUtils.format("%s|%s", datasource, type);
  }

  public DatastoreWatermarkStoreConfig getConfig()
  {
    return datastoreConfig;
  }

  @Override
  public void update(String datasource, String type, DateTime timestamp)
  {
    final String key = keyGen(datasource, type);
    final long tsMillis = timestamp.getMillis();
    final AtomicLong priorMaxAtomic = maxStore.computeIfAbsent(key, k -> new AtomicLong(tsMillis - 1));
    final long priorMax = priorMaxAtomic.get();
    if (priorMax < tsMillis) {
      try {
        log.debug("Updating %s timeline watermark %s to %s", datasource, type, timestamp);
        final Key preKey = new Key(
        ).setPartitionId(
            new PartitionId(
            ).setNamespaceId(
                datastoreConfig.getNamespace()
            ).setProjectId(
                datastoreConfig.getProjectId())
        ).setPath(
            Collections.singletonList(new PathElement().setKind(KIND))
        );
        final BeginTransactionRequest beginTransactionRequest = new BeginTransactionRequest()
            .setTransactionOptions(new TransactionOptions().setReadWrite(new ReadWrite()));
        final BeginTransactionResponse beginTransactionResponse = datastore
            .projects()
            .beginTransaction(
                datastoreConfig.getProjectId(),
                beginTransactionRequest
            )
            .execute();
        final CommitRequest commitRequest = new CommitRequest()
            .setTransaction(beginTransactionResponse.getTransaction())
            .setMutations(
                Collections.singletonList(
                    new Mutation().setInsert(
                        new Entity()
                            .setKey(preKey)
                            .setProperties(ImmutableMap.of(
                                TYPE_COLUMN,
                                new Value().setStringValue(type),
                                DATASOURCE,
                                new Value().setStringValue(datasource),
                                INSERT_TIMESTAMP_COLUMN,
                                new Value().setTimestampValue(DateTimes.nowUtc()
                                                                       .toString(ISODateTimeFormat.dateTime())),
                                TIMESTAMP_COLUMN,
                                new Value().setTimestampValue(timestamp.toString(ISODateTimeFormat.dateTime()))
                            ))
                    )
                )
            );
        final CommitResponse commitResponse = datastore
            .projects()
            .commit(datastoreConfig.getProjectId(), commitRequest)
            .execute();
        if (commitResponse
            .getMutationResults()
            .stream()
            .map(MutationResult::getConflictDetected)
            .anyMatch(Objects::nonNull)) {
          log.warn("Failed to update watermark! %s", commitResponse.getMutationResults().get(0));
        }
        priorMaxAtomic.set(tsMillis);
      }
      catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }

  @Override
  public void rollback(String datasource, String type, DateTime timestamp)
  {
    final String key = keyGen(datasource, type);
    long previous = maxStore.computeIfAbsent(key, k -> new AtomicLong()).getAndUpdate(
        currentMax -> timestamp.getMillis()
    );
    log.debug(
        "Rolling back %s timeline watermark %s to %s from %s",
        datasource,
        type,
        timestamp,
        DateTimes.utc(previous)
    );
    purgeInterval(datasource, type, timestamp.plus(1), TIMESTAMP_MAX);
  }

  @Override
  public void purgeHistory(String datasource, String type, DateTime timestamp)
  {
    final String key = keyGen(datasource, type);
    maxStore.remove(key);
    log.debug(
        "Purging history for %s timeline watermark %s older than %s",
        datasource,
        type,
        timestamp
    );
    purgeInterval(datasource, type, TIMESTAMP_MIN, timestamp);
  }

  private void purgeInterval(String datasource, String type, DateTime start, DateTime end)
  {
    String startCursor = null;
    do {
      final QueryResultBatch results;
      try {
        results = getEntitiesInRange(
            datasource,
            type,
            Intervals.utc(start.getMillis(), end.getMillis()),
            INSERT_TIMESTAMP_COLUMN,
            startCursor
        );
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (results.getEntityResults() == null) {
        return;
      }
      final List<Mutation> mutations = results
          .getEntityResults()
          .stream()
          .map(EntityResult::getEntity)
          .map(Entity::getKey)
          .map(k -> new Mutation().setDelete(k))
          .collect(Collectors.toList());
      final BeginTransactionResponse beginTransactionResponse;
      try {
        beginTransactionResponse = datastore.projects(
        ).beginTransaction(
            datastoreConfig.getProjectId(),
            new BeginTransactionRequest(
            ).setTransactionOptions(
                new TransactionOptions().setReadWrite(new ReadWrite())
            )
        ).execute();
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }

      if (!mutations.isEmpty()) {
        final CommitRequest commitRequest = new CommitRequest(
        ).setTransaction(
            beginTransactionResponse.getTransaction()
        ).setMutations(
            mutations
        );
        final CommitResponse commitResponse;
        try {
          commitResponse = datastore.projects().commit(datastoreConfig.getProjectId(), commitRequest).execute();
        }
        catch (IOException e) {
          throw new RuntimeException(e);
        }
        log.debug(
            "Deleted %d keys from %s : %s",
            commitResponse.getIndexUpdates(),
            datastoreConfig.getProjectId(),
            type
        );
      }

      if ("MORE_RESULTS_AFTER_LIMIT".equals(results.getMoreResults())) {
        startCursor = results.getEndCursor();
      } else {
        startCursor = null;
      }
    } while (startCursor != null);
  }

  @Override
  @Nullable
  public DateTime getValue(String datasource, String type)
  {
    final Query query = new Query(
    ).setKind(
        Collections.singletonList(new KindExpression().setName(KIND))
    ).setFilter(
        new Filter(
        ).setCompositeFilter(
            new CompositeFilter(
            ).setOp(
                "AND"
            ).setFilters(
                ImmutableList.of(
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(DATASOURCE)
                        ).setOp(
                            "EQUAL"
                        ).setValue(
                            new Value().setStringValue(datasource)
                        )
                    ),
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(TYPE_COLUMN)
                        ).setOp(
                            "EQUAL"
                        ).setValue(
                            new Value().setStringValue(type)
                        )
                    )
                )
            )
        )
    ).setProjection(
        ImmutableList.of(
            new Projection().setProperty(new PropertyReference().setName(TIMESTAMP_COLUMN))
        )
    ).setOrder(
        ImmutableList.of(
            new PropertyOrder(
            ).setDirection(
                "DESCENDING"
            ).setProperty(
                new PropertyReference().setName(TIMESTAMP_COLUMN)
            )
        )
    ).setLimit(
        1
    );
    log.debug("Fetching %s timeline watermark %s", datasource, type);
    final RunQueryRequest runQueryRequest = new RunQueryRequest(
    ).setPartitionId(
        new PartitionId().setProjectId(datastoreConfig.getProjectId()).setNamespaceId(datastoreConfig.getNamespace())
    ).setQuery(
        query
    );
    final RunQueryResponse queryResponse;
    try {
      queryResponse = datastore
          .projects()
          .runQuery(datastoreConfig.getProjectId(), runQueryRequest)
          .execute();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (queryResponse.getBatch() == null || queryResponse.getBatch().getEntityResults() == null) {
      return null;
    }
    final List<DateTime> results = queryResponse
        .getBatch()
        .getEntityResults()
        .stream()
        .map(EntityResult::getEntity)
        .map(Entity::getProperties)
        .map(p -> p.get(TIMESTAMP_COLUMN))
        .filter(Objects::nonNull)
        .map(Value::getIntegerValue)
        .map(l -> l / 1000) // kept in us instead of ms
        .map(DateTimes::utc)
        .collect(Collectors.toList());
    if (results.isEmpty()) {
      return null;
    }
    return results.get(0);
  }

  @Override
  public Collection<String> getDatasources()
  {
    log.debug("Fetching datasources");
    final Query query = new Query(
    ).setKind(
        Collections.singletonList(new KindExpression().setName(KIND))
    ).setProjection(
        ImmutableList.of(
            new Projection().setProperty(new PropertyReference().setName(DATASOURCE))
        )
    ).setDistinctOn(
        Collections.singletonList(
            new PropertyReference().setName(DATASOURCE)
        )
    );

    final RunQueryRequest runQueryRequest = new RunQueryRequest(
    ).setPartitionId(
        new PartitionId().setProjectId(datastoreConfig.getProjectId()).setNamespaceId(datastoreConfig.getNamespace())
    ).setQuery(
        query
    );
    final RunQueryResponse response;
    try {
      response = datastore
          .projects()
          .runQuery(datastoreConfig.getProjectId(), runQueryRequest)
          .execute();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }

    if (response.getBatch() == null || response.getBatch().getEntityResults() == null) {
      return null;
    }

    return response
        .getBatch()
        .getEntityResults()
        .stream()
        .map(
            EntityResult::getEntity
        ).map(
            Entity::getProperties
        ).map(
            m -> m.get(DATASOURCE).getStringValue()
        ).collect(
            Collectors.toList()
        );
  }

  @Override
  public Map<String, DateTime> getValues(String datasource)
  {
    log.debug("Fetching %s timeline watermarks", datasource);
    final Query query = new Query(
    ).setKind(
        Collections.singletonList(new KindExpression().setName(KIND))
    ).setFilter(
        new Filter(
        ).setPropertyFilter(
            new PropertyFilter(
            ).setProperty(
                new PropertyReference().setName(DATASOURCE)
            ).setOp(
                "EQUAL"
            ).setValue(
                new Value().setStringValue(datasource)
            )
        )
    ).setProjection(
        ImmutableList.of(
            new Projection().setProperty(new PropertyReference().setName(TYPE_COLUMN)),
            new Projection().setProperty(new PropertyReference().setName(TIMESTAMP_COLUMN))
        )
    ).setDistinctOn(
        Collections.singletonList(
            new PropertyReference().setName(TYPE_COLUMN)
        )
    ).setOrder(
        ImmutableList.of(
            new PropertyOrder(
            ).setDirection(
                "ASCENDING"
            ).setProperty(
                new PropertyReference().setName(TYPE_COLUMN)
            ),
            new PropertyOrder(
            ).setDirection(
                "DESCENDING"
            ).setProperty(
                new PropertyReference().setName(TIMESTAMP_COLUMN)
            )
        )
    ).setLimit(
        500 // get a bunch, should never have that much
    );
    final RunQueryRequest runQueryRequest = new RunQueryRequest(
    ).setPartitionId(
        new PartitionId().setProjectId(datastoreConfig.getProjectId()).setNamespaceId(datastoreConfig.getNamespace())
    ).setQuery(
        query
    );
    final RunQueryResponse response;
    try {
      response = datastore
          .projects()
          .runQuery(datastoreConfig.getProjectId(), runQueryRequest)
          .execute();
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (response.getBatch() == null || response.getBatch().getEntityResults() == null) {
      return null;
    }

    return response
        .getBatch()
        .getEntityResults()
        .stream()
        .map(
            EntityResult::getEntity
        ).collect(
            Collectors.toMap(
                e -> e.getProperties().get(TYPE_COLUMN).getStringValue(),
                e -> DateTimes.utc(e.getProperties().get(TIMESTAMP_COLUMN).getIntegerValue() / 1000)
            )
        );
  }

  @Override
  public List<Pair<DateTime, DateTime>> getValueHistory(String datasource, String type, Interval range)
  {
    log.debug("Fetching %s timeline watermark history for %s between %s and %s",
              datasource, type, range.getStart(), range.getEnd()
    );

    String startCursor = null;

    final List<Pair<DateTime, DateTime>> timeline = new ArrayList<>();

    do {
      final QueryResultBatch results;
      try {
        results = getEntitiesInRange(datasource, type, range, INSERT_TIMESTAMP_COLUMN, startCursor);
      }
      catch (IOException e) {
        throw new RuntimeException(e);
      }
      if (results.getEntityResults() == null) {
        return timeline;
      }
      results.getEntityResults().stream().map(EntityResult::getEntity).forEach(e -> timeline.add(
          new Pair<>(
              DateTimes.of(e.getProperties().get(TIMESTAMP_COLUMN).getTimestampValue()),
              DateTimes.of(e.getProperties().get(INSERT_TIMESTAMP_COLUMN).getTimestampValue())
          )
      ));
      if ("MORE_RESULTS_AFTER_LIMIT".equals(results.getMoreResults())) {
        startCursor = results.getEndCursor();
      } else {
        startCursor = null;
      }
    } while (startCursor != null);

    return timeline;
  }

  @Override
  public void initialize()
  {

  }

  private QueryResultBatch getEntitiesInRange(
      String datasource,
      String type,
      Interval range,
      String column,
      String startCursor
  ) throws IOException
  {
    final Query query = new Query(
    ).setStartCursor(
        startCursor
    ).setKind(
        Collections.singletonList(new KindExpression().setName(KIND))
    ).setLimit(
        keeperConfig.getMaxHistoryResults()
    ).setFilter(
        new Filter().setCompositeFilter(
            new CompositeFilter(
            ).setOp(
                "AND"
            ).setFilters(
                ImmutableList.of(
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(DATASOURCE)
                        ).setValue(
                            new Value().setStringValue(datasource)
                        ).setOp("EQUAL")
                    ),
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(TYPE_COLUMN)
                        ).setValue(
                            new Value().setStringValue(type)
                        ).setOp("EQUAL")
                    ),
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(column)
                        ).setValue(
                            new Value().setTimestampValue(range.getStart().toString(ISODateTimeFormat.dateTime()))
                        ).setOp("GREATER_THAN_OR_EQUAL")
                    ),
                    new Filter(
                    ).setPropertyFilter(
                        new PropertyFilter(
                        ).setProperty(
                            new PropertyReference().setName(column)
                        ).setValue(
                            new Value().setTimestampValue(range.getEnd().toString(ISODateTimeFormat.dateTime()))
                        ).setOp("LESS_THAN")
                    )
                )
            )
        )
    );
    final RunQueryRequest runQueryRequest = new RunQueryRequest(
    ).setPartitionId(
        new PartitionId().setProjectId(datastoreConfig.getProjectId()).setNamespaceId(datastoreConfig.getNamespace())
    ).setQuery(
        query
    );
    final RunQueryResponse runQueryResponse = datastore
        .projects()
        .runQuery(datastoreConfig.getProjectId(), runQueryRequest)
        .execute();
    return runQueryResponse.getBatch();
  }
}
