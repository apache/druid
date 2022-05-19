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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import org.apache.druid.guice.ManageLifecycle;
import org.apache.druid.indexing.materializedview.DerivativeDataSourceMetadata;
import org.apache.druid.indexing.overlord.DataSourceMetadata;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.jackson.JacksonUtils;
import org.apache.druid.java.util.common.lifecycle.LifecycleStart;
import org.apache.druid.java.util.common.lifecycle.LifecycleStop;
import org.apache.druid.java.util.emitter.EmittingLogger;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SQLMetadataConnector;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.tweak.HandleCallback;

import java.sql.ResultSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Read and store derivatives information from dataSource table frequently.
 * When optimize query, DerivativesManager offers the information about derivatives.
 */
@ManageLifecycle
public class DerivativeDataSourceManager 
{
  private static final EmittingLogger log = new EmittingLogger(DerivativeDataSourceManager.class);
  private static final AtomicReference<ConcurrentHashMap<String, SortedSet<DerivativeDataSource>>> DERIVATIVES_REF =
      new AtomicReference<>(new ConcurrentHashMap<>());
  private final MaterializedViewConfig config;
  private final Supplier<MetadataStorageTablesConfig> dbTables;
  private final SQLMetadataConnector connector;
  private final ObjectMapper objectMapper;
  private final Object lock = new Object();
  
  private boolean started = false;
  private ListeningScheduledExecutorService exec = null;
  private ListenableFuture<?> future = null;
  
  @Inject
  public DerivativeDataSourceManager(
      MaterializedViewConfig config,
      Supplier<MetadataStorageTablesConfig> dbTables,
      ObjectMapper objectMapper,
      SQLMetadataConnector connector
  )
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
      exec = MoreExecutors.listeningDecorator(Execs.scheduledSingleThreaded("DerivativeDataSourceManager-Exec-%d"));
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
      DERIVATIVES_REF.set(new ConcurrentHashMap<>());
      exec.shutdownNow();
      exec = null;
    }
  }

  public static ImmutableSet<DerivativeDataSource> getDerivatives(String datasource)
  {
    return ImmutableSet.copyOf(DERIVATIVES_REF.get().getOrDefault(datasource, new TreeSet<>()));
  }

  public static ImmutableMap<String, Set<DerivativeDataSource>> getAllDerivatives()
  {
    return ImmutableMap.copyOf(DERIVATIVES_REF.get());
  }

  private void updateDerivatives()
  {
    List<Pair<String, DerivativeDataSourceMetadata>> derivativesInDatabase = connector.retryWithHandle(
        handle ->
          handle
              .createQuery(
                  StringUtils.format(
                      "SELECT DISTINCT dataSource,commit_metadata_payload FROM %1$s",
                      dbTables.get().getDataSourceTable()
                  )
              )
              .map((int index, ResultSet r, StatementContext ctx) -> {
                String datasourceName = r.getString("dataSource");
                DataSourceMetadata payload = JacksonUtils.readValue(
                    objectMapper,
                    r.getBytes("commit_metadata_payload"),
                    DataSourceMetadata.class);
                if (!(payload instanceof DerivativeDataSourceMetadata)) {
                  return null;
                }
                DerivativeDataSourceMetadata metadata = (DerivativeDataSourceMetadata) payload;
                return new Pair<>(datasourceName, metadata);
              })
              .list()
    );
    
    List<DerivativeDataSource> derivativeDataSources = derivativesInDatabase.parallelStream()
        .filter(data -> data != null)
        .map(derivatives -> {
          String name = derivatives.lhs;
          DerivativeDataSourceMetadata metadata = derivatives.rhs;
          String baseDataSource = metadata.getBaseDataSource();
          long avgSizePerGranularity = getAvgSizePerGranularity(name);
          log.info("find derivatives: {bases=%s, derivative=%s, dimensions=%s, metrics=%s, avgSize=%s}", 
              baseDataSource, name, metadata.getDimensions(), metadata.getMetrics(), avgSizePerGranularity);
          return new DerivativeDataSource(name, baseDataSource, metadata.getColumns(), avgSizePerGranularity);
        })
        .filter(derivatives -> derivatives.getAvgSizeBasedGranularity() > 0)
        .collect(Collectors.toList());
    
    ConcurrentHashMap<String, SortedSet<DerivativeDataSource>> newDerivatives = new ConcurrentHashMap<>();
    for (DerivativeDataSource derivative : derivativeDataSources) {
      newDerivatives.computeIfAbsent(derivative.getBaseDataSource(), ds -> new TreeSet<>()).add(derivative);
    }
    ConcurrentHashMap<String, SortedSet<DerivativeDataSource>> current;
    do {
      current = DERIVATIVES_REF.get();
    } while (!DERIVATIVES_REF.compareAndSet(current, newDerivatives));
  }

  /**
   * calculate the average data size per segment granularity for a given datasource.
   * 
   * e.g. for a datasource, there're 5 segments as follows,
   * interval = "2018-04-01/2017-04-02", segment size = 1024 * 1024 * 2
   * interval = "2018-04-01/2017-04-02", segment size = 1024 * 1024 * 2
   * interval = "2018-04-02/2017-04-03", segment size = 1024 * 1024 * 1
   * interval = "2018-04-02/2017-04-03", segment size = 1024 * 1024 * 1
   * interval = "2018-04-02/2017-04-03", segment size = 1024 * 1024 * 1
   * Then, we get interval number = 2, total segment size = 1024 * 1024 * 7
   * At last, return the result 1024 * 1024 * 7 / 2 = 1024 * 1024 * 3.5
   * 
   * @param datasource
   * @return average data size per segment granularity
   */
  private long getAvgSizePerGranularity(String datasource)
  {
    return connector.retryWithHandle(
        new HandleCallback<Long>() {
          Set<Interval> intervals = new HashSet<>();
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
                    (int index, ResultSet r, StatementContext ctx) -> {
                      intervals.add(
                          Intervals.utc(
                              DateTimes.of(r.getString("start")).getMillis(),
                              DateTimes.of(r.getString("end")).getMillis()
                          )
                      );
                      DataSegment segment =
                          JacksonUtils.readValue(objectMapper, r.getBytes("payload"), DataSegment.class);
                      totalSize += segment.getSize();
                      return null;
                    }
                )
                .list();
            return intervals.isEmpty() ? 0L : totalSize / intervals.size();
          }
        }
    );
  }
}
