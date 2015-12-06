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

package io.druid.segment.realtime;


import com.google.common.base.Function;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.inject.Inject;
import com.metamx.common.guava.CloseQuietly;
import com.metamx.common.lifecycle.LifecycleStart;
import com.metamx.common.lifecycle.LifecycleStop;
import com.metamx.common.parsers.ParseException;
import com.metamx.emitter.EmittingLogger;
import io.druid.data.input.Committer;
import io.druid.data.input.Firehose;
import io.druid.data.input.FirehoseV2;
import io.druid.data.input.InputRow;
import io.druid.query.FinalizeResultsQueryRunner;
import io.druid.query.NoopQueryRunner;
import io.druid.query.Query;
import io.druid.query.QueryRunner;
import io.druid.query.QueryRunnerFactory;
import io.druid.query.QueryRunnerFactoryConglomerate;
import io.druid.query.QuerySegmentWalker;
import io.druid.query.QueryToolChest;
import io.druid.query.SegmentDescriptor;
import io.druid.segment.incremental.IndexSizeExceededException;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.Plumber;
import org.joda.time.Interval;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 */
public class RealtimeManager implements QuerySegmentWalker
{
  private static final EmittingLogger log = new EmittingLogger(RealtimeManager.class);

  private final List<FireDepartment> fireDepartments;
  private final QueryRunnerFactoryConglomerate conglomerate;

  /**
   * key=data source name,value=FireChiefs of all partition of that data source
   */
  private final Map<String, List<FireChief>> chiefs;

  @Inject
  public RealtimeManager(
      List<FireDepartment> fireDepartments,
      QueryRunnerFactoryConglomerate conglomerate
  )
  {
    this.fireDepartments = fireDepartments;
    this.conglomerate = conglomerate;

    this.chiefs = Maps.newHashMap();
  }

  @LifecycleStart
  public void start() throws IOException
  {
    for (final FireDepartment fireDepartment : fireDepartments) {
      DataSchema schema = fireDepartment.getDataSchema();

      final FireChief chief = new FireChief(fireDepartment);
      List<FireChief> chiefs = this.chiefs.get(schema.getDataSource());
      if (chiefs == null) {
        chiefs = new ArrayList<FireChief>();
        this.chiefs.put(schema.getDataSource(), chiefs);
      }
      chiefs.add(chief);

      chief.setName(
          String.format(
              "chief-%s[%s]",
              schema.getDataSource(),
              fireDepartment.getTuningConfig().getShardSpec().getPartitionNum()
          )
      );
      chief.setDaemon(true);
      chief.start();
    }
  }

  @LifecycleStop
  public void stop()
  {
    for (Iterable<FireChief> chiefs : this.chiefs.values()) {
      for (FireChief chief : chiefs) {
        CloseQuietly.close(chief);
      }
    }
  }

  public FireDepartmentMetrics getMetrics(String datasource)
  {
    List<FireChief> chiefs = this.chiefs.get(datasource);
    if (chiefs == null) {
      return null;
    }
    FireDepartmentMetrics snapshot = null;
    for (FireChief chief : chiefs) {
      if (snapshot == null) {
        snapshot = chief.getMetrics().snapshot();
      } else {
        snapshot.merge(chief.getMetrics());
      }
    }
    return snapshot;
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForIntervals(final Query<T> query, Iterable<Interval> intervals)
  {
    return getQueryRunnerForSegments(query, null);
  }

  @Override
  public <T> QueryRunner<T> getQueryRunnerForSegments(final Query<T> query, Iterable<SegmentDescriptor> specs)
  {
    final QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);

    Iterable<FireChief> chiefsOfDataSource = chiefs.get(Iterables.getOnlyElement(query.getDataSource().getNames()));
    return chiefsOfDataSource == null ? new NoopQueryRunner() : factory.getToolchest().mergeResults(
        factory.mergeRunners(
            MoreExecutors.sameThreadExecutor(),
            // Chaining query runners which wait on submitted chain query runners can make executor pools deadlock
            Iterables.transform(
                chiefsOfDataSource, new Function<FireChief, QueryRunner<T>>()
                {
                  @Override
                  public QueryRunner<T> apply(FireChief input)
                  {
                    return input.getQueryRunner(query);
                  }
                }
            )
        )
    );
  }

  private class FireChief extends Thread implements Closeable
  {
    private final FireDepartment fireDepartment;
    private final FireDepartmentMetrics metrics;
    private final RealtimeTuningConfig config;

    private volatile Firehose firehose = null;
    private volatile FirehoseV2 firehoseV2 = null;
    private volatile Plumber plumber = null;
    private volatile boolean normalExit = true;

    public FireChief(
        FireDepartment fireDepartment
    )
    {
      this.fireDepartment = fireDepartment;
      this.config = fireDepartment.getTuningConfig();
      this.metrics = fireDepartment.getMetrics();
    }

    public Firehose initFirehose()
    {
      synchronized (this) {
        if (firehose == null) {
          try {
            log.info("Calling the FireDepartment and getting a Firehose.");
            firehose = fireDepartment.connect();
            log.info("Firehose acquired!");
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
        } else {
          log.warn("Firehose already connected, skipping initFirehose().");
        }

        return firehose;
      }
    }

    public FirehoseV2 initFirehoseV2(Object metaData)
    {
      synchronized (this) {
        if (firehoseV2 == null) {
          try {
            log.info("Calling the FireDepartment and getting a FirehoseV2.");
            firehoseV2 = fireDepartment.connect(metaData);
            log.info("FirehoseV2 acquired!");
          }
          catch (IOException e) {
            throw Throwables.propagate(e);
          }
        } else {
          log.warn("FirehoseV2 already connected, skipping initFirehoseV2().");
        }

        return firehoseV2;
      }
    }

    public Plumber initPlumber()
    {
      synchronized (this) {
        if (plumber == null) {
          log.info("Someone get us a plumber!");
          plumber = fireDepartment.findPlumber();
          log.info("We have our plumber!");
        } else {
          log.warn("Plumber already trained, skipping initPlumber().");
        }

        return plumber;
      }
    }

    public FireDepartmentMetrics getMetrics()
    {
      return metrics;
    }

    @Override
    public void run()
    {
      plumber = initPlumber();

      try {
        Object metadata = plumber.startJob();

        if (fireDepartment.checkFirehoseV2()) {
          firehoseV2 = initFirehoseV2(metadata);
          runFirehoseV2(firehoseV2);
        } else {
          firehose = initFirehose();
          runFirehose(firehose);
        }

      }
      catch (RuntimeException e) {
        log.makeAlert(
            e,
            "RuntimeException aborted realtime processing[%s]",
            fireDepartment.getDataSchema().getDataSource()
        ).emit();
        normalExit = false;
        throw e;
      }
      catch (Error e) {
        log.makeAlert(e, "Exception aborted realtime processing[%s]", fireDepartment.getDataSchema().getDataSource())
           .emit();
        normalExit = false;
        throw e;
      }
      finally {
        CloseQuietly.close(firehose);
        if (normalExit) {
          plumber.finishJob();
          plumber = null;
          firehose = null;
        }
      }
    }

    private void runFirehoseV2(FirehoseV2 firehose)
    {
      try {
        firehose.start();
      }
      catch (Exception e) {
        log.error(e, "Failed to start firehoseV2");
        return;
      }

      log.info("FirehoseV2 started");
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehoseV2(firehose);
      boolean haveRow = true;
      while (haveRow) {
        InputRow inputRow = null;
        int numRows = 0;
        try {
          inputRow = firehose.currRow();
          if (inputRow != null) {
            numRows = plumber.add(inputRow, committerSupplier);
            if (numRows < 0) {
              metrics.incrementThrownAway();
              log.debug("Throwing away event[%s]", inputRow);
            } else {
              metrics.incrementProcessed();
            }
          } else {
            log.debug("thrown away null input row, considering unparseable");
            metrics.incrementUnparseable();
          }
        }
        catch (Exception e) {
          log.makeAlert(e, "Unknown exception, Ignoring and continuing.")
             .addData("inputRow", inputRow);
        }

        try {
          haveRow = firehose.advance();
        }
        catch (Exception e) {
          log.debug(e, "exception in firehose.advance(), considering unparseable row");
          metrics.incrementUnparseable();
        }
      }
    }

    private void runFirehose(Firehose firehose)
    {
      final Supplier<Committer> committerSupplier = Committers.supplierFromFirehose(firehose);
      while (firehose.hasMore()) {
        final InputRow inputRow;
        try {
          inputRow = firehose.nextRow();

          if (inputRow == null) {
            log.debug("thrown away null input row, considering unparseable");
            metrics.incrementUnparseable();
            continue;
          }
        }
        catch (ParseException e) {
          log.debug(e, "thrown away line due to exception, considering unparseable");
          metrics.incrementUnparseable();
          continue;
        }

        boolean lateEvent = false;
        boolean indexLimitExceeded = false;
        try {
          lateEvent = plumber.add(inputRow, committerSupplier) == -1;
        }
        catch (IndexSizeExceededException e) {
          log.info("Index limit exceeded: %s", e.getMessage());
          indexLimitExceeded = true;
        }
        if (indexLimitExceeded || lateEvent) {
          metrics.incrementThrownAway();
          log.debug("Throwing away event[%s]", inputRow);

          if (indexLimitExceeded) {
            plumber.persist(committerSupplier.get());
          }

          continue;
        }
        metrics.incrementProcessed();
      }
    }

    public <T> QueryRunner<T> getQueryRunner(Query<T> query)
    {
      QueryRunnerFactory<T, Query<T>> factory = conglomerate.findFactory(query);
      QueryToolChest<T, Query<T>> toolChest = factory.getToolchest();

      return new FinalizeResultsQueryRunner<T>(plumber.getQueryRunner(query), toolChest);
    }

    public void close() throws IOException
    {
      synchronized (this) {
        if (firehose != null) {
          normalExit = false;
          firehose.close();
        }
      }
    }
  }
}
