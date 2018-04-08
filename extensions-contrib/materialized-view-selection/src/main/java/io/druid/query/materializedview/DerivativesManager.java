/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package io.druid.query.materializedview;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import io.druid.guice.ManageLifecycleLast;
import io.druid.indexing.overlord.DataSourceMetadata;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.indexing.materializedview.DerivativeDataSourceMetadata;
import io.druid.java.util.common.DateTimes;
import io.druid.java.util.common.Intervals;
import io.druid.java.util.common.Pair;
import io.druid.java.util.common.StringUtils;
import io.druid.java.util.common.concurrent.Execs;
import io.druid.java.util.common.lifecycle.LifecycleStart;
import io.druid.java.util.common.lifecycle.LifecycleStop;
import io.druid.java.util.emitter.EmittingLogger;
import io.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Read and store derivatives information from dataSource table frequently.
 * When optimize query, DerivativesManager offers the information about derivatives.
 */
@ManageLifecycleLast
public class DerivativesManager 
{
  private static final EmittingLogger log = new EmittingLogger(DerivativesManager.class);
  private final MaterializedViewConfig config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final ObjectMapper objectMapper;
  private final Object lock = new Object();
  
  private static volatile AtomicReference<ConcurrentHashMap<String, SortedSet<Derivative>>> derivativesRef = new AtomicReference<>(new ConcurrentHashMap<>());
  private volatile boolean started = false;
  private volatile ListeningScheduledExecutorService exec = null;
  private volatile ListenableFuture<?> future = null;
  
  @Inject
  public DerivativesManager(
      MaterializedViewConfig config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      ObjectMapper objectMapper,
      SQLMetadataConnector connector)
  {
    this.config = config;
    this.dbTables = dbTables;
    this.objectMapper = objectMapper;
    this.connector = connector;
  }

  @LifecycleStart
  public void start()
  {
    log.info("starting derivatives manager.");
    synchronized (lock) {
      if (started) {
        return;
      }
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("SQLMetadataDerivativesManager-Exec--%d"));
      final Duration delay = config.getPollDuration().toStandardDuration();
      future = exec.scheduleWithFixedDelay(
          new Runnable() {
            @Override
            public void run() 
            {
              try {
                updateDerivatives();
              }
              catch (Exception e) {
                log.makeAlert(e, "uncaught exception in derivatives manager updating thread").emit();
              }
            }
          },
          0,
          delay.getMillis(),
          TimeUnit.MILLISECONDS
      );
      started = true;
    }
    log.info("Derivatives manager started.");
  }

  @LifecycleStop
  public void stop()
  {
    synchronized (lock) {
      if (!started) {
        return;
      }
      started = false;
      future.cancel(true);
      future = null;
      final ConcurrentHashMap<String, SortedSet<Derivative>> emptyMap = new ConcurrentHashMap<>();
      ConcurrentHashMap<String, SortedSet<Derivative>> current;
      do {
        current = derivativesRef.get();
      } while (!derivativesRef.compareAndSet(current, emptyMap));
      exec.shutdownNow();
      exec = null;
    }
  }

  public static ImmutableSortedSet<Derivative> getDerivatives(String datasource)
  {
    return ImmutableSortedSet.copyOf(derivativesRef.get().getOrDefault(datasource, Sets.newTreeSet()));
  }

  public static ImmutableMap getAllDerivatives()
  {
    return ImmutableMap.copyOf(derivativesRef.get());
  }

  private void updateDerivatives()
  {
    List<Pair<String, Derivative>> derivativesInDatabase = connector.retryWithHandle(
        handle ->
          handle.createQuery(
              StringUtils.format("SELECT DISTINCT dataSource,commit_metadata_payload FROM %1$s", dbTables.get().getDataSourceTable())
          )
              .map(new ResultSetMapper<Pair<String, Derivative>>() 
              {
                @Override 
                public Pair<String, Derivative> map(int index, ResultSet r, StatementContext ctx) throws SQLException 
                {
                  String datasourceName = r.getString("dataSource");
                  try {
                    DataSourceMetadata payload = objectMapper.readValue(
                        r.getBytes("commit_metadata_payload"),
                        DataSourceMetadata.class);
                    if (!(payload instanceof DerivativeDataSourceMetadata)) {
                      return null;
                    }
                    DerivativeDataSourceMetadata metadata = (DerivativeDataSourceMetadata) payload;
                    long avgSizePerGranularity = getAvgSizePerGranularity(datasourceName);
                    String baseDataSource = metadata.getBaseDataSource();
                    log.info("find derivatives: {bases=%s, derivative=%s, dimensions=%s, metrics=%s, avgSize=%s}", baseDataSource, 
                        datasourceName, metadata.getDimensions(), metadata.getMetrics(), avgSizePerGranularity);
                    if (avgSizePerGranularity > 0) {
                      return new Pair<>(baseDataSource, new Derivative(datasourceName, metadata.getColumns(), avgSizePerGranularity));
                    }
                  }
                  catch (IOException e) {
                    throw new RuntimeException(e);
                  }
                  return null;
                }
              })
              .list()
    );
    ConcurrentHashMap<String, SortedSet<Derivative>> newDerivatives = new ConcurrentHashMap<>();
    for (Pair<String, Derivative> derivativesPair : derivativesInDatabase) {
      if (derivativesPair == null) {
        continue;
      }
      newDerivatives.putIfAbsent(derivativesPair.lhs, Sets.newTreeSet());
      newDerivatives.get(derivativesPair.lhs).add(derivativesPair.rhs);
    }
    ConcurrentHashMap<String, SortedSet<Derivative>> current;
    do {
      current = derivativesRef.get();
    } while (!derivativesRef.compareAndSet(current, newDerivatives));
  }

  private long getAvgSizePerGranularity(String datasource)
  {
    return connector.retryWithHandle(
        new HandleCallback<Long>() {
          Set<Interval> intervals = Sets.newHashSet();
          long totalSize = 0;
          @Override
          public Long withHandle(Handle handle)
          {
            handle.createQuery(
                StringUtils.format("SELECT start,%1$send%1$s,payload FROM %2$s WHERE used = true AND dataSource = :dataSource",
                connector.getQuoteString(), dbTables.get().getSegmentsTable()
                )
            )
                .bind("dataSource", datasource)
                .map(
                    new ResultSetMapper<Object>() 
                    {
                      @Override
                      public Object map(int index, ResultSet r, StatementContext ctx) throws SQLException 
                      {
                        try {
                          intervals.add(Intervals.utc(DateTimes.of(r.getString("start")).getMillis(), DateTimes.of(r.getString("end")).getMillis()));
                          DataSegment segment = objectMapper.readValue(r.getBytes("payload"), DataSegment.class);
                          totalSize += segment.getSize();
                        } 
                        catch (IOException e) {
                          throw new RuntimeException(e);
                        }
                        return null;
                      }
                    }
                )
                .first();
            return intervals.isEmpty() ? 0L : totalSize / intervals.size();
          }
        }
    );
  }
}
