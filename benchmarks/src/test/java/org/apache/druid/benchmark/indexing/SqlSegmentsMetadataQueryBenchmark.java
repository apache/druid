package org.apache.druid.benchmark.indexing;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.IndexerSqlMetadataStorageCoordinatorTestBase;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.storage.derby.DerbyConnector;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 1, time = 1)
@Measurement(iterations = 20, time = 2)
@BenchmarkMode({Mode.AverageTime})
@OutputTimeUnit(TimeUnit.NANOSECONDS)
public class SqlSegmentsMetadataQueryBenchmark {

  private static final DateTime JAN_1 = DateTimes.of("2025-01-01");
  private static final String V1 = JAN_1.toString();
  private static final List<DataSegment> WIKI_SEGMENTS_2X5D
          = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
          .forIntervals(10, Granularities.DAY)
          .withNumPartitions(20)
          .startingAt(JAN_1)
          .withVersion(V1)
          .eachOfSizeInMb(500);
  @Param({
          "2025-01-01T00:00:00.000Z/2025-01-02T00:00:00.000Z",
          "2025-01-09T00:00:00.000Z/2025-01-10T00:00:00.000Z",
          "2025-01-01T00:00:00.000Z/2025-01-05T00:00:00.000Z",
          "2025-01-01T00:00:00.000Z/2025-01-10T00:00:00.000Z",
  })
  private String queryIntervalStr;
  private Interval queryInterval;
  private TestDerbyConnector.DerbyConnectorRule derbyConnectorRule;

  @Setup(Level.Trial)
  public void setup() throws Exception {
    this.derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();
    derbyConnectorRule.beforeBenchmark();
    derbyConnectorRule.getConnector().createSegmentTable();
    insertSegments(WIKI_SEGMENTS_2X5D.toArray(new DataSegment[0]));
    this.queryInterval = new Interval(this.queryIntervalStr);
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception {
    derbyConnectorRule.afterBenchmark();
  }

  @Benchmark
  public void benchmarkRetrieveUsedSegments(Blackhole blackhole) {
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(this.queryInterval))));
  }

  private <T> Set<T> readAsSet(Function<SqlSegmentsMetadataQuery, CloseableIterator<T>> iterableReader) {
    final DerbyConnector connector = derbyConnectorRule.getConnector();
    final MetadataStorageTablesConfig tablesConfig = derbyConnectorRule.metadataTablesConfigSupplier().get();

    return connector.inReadOnlyTransaction((handle, status) -> {
      final SqlSegmentsMetadataQuery query =
              SqlSegmentsMetadataQuery.forHandle(handle, connector, tablesConfig, TestHelper.JSON_MAPPER);

      try (CloseableIterator<T> iterator = iterableReader.apply(query)) {
        return ImmutableSet.copyOf(iterator);
      }
    });
  }

  private void insertSegments(DataSegment... segments) {
    IndexerSqlMetadataStorageCoordinatorTestBase.insertUsedSegments(
            Set.of(segments),
            Map.of(),
            derbyConnectorRule,
            TestHelper.JSON_MAPPER
    );
  }
}
