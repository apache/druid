package org.apache.druid.benchmark.indexing;

import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.parsers.CloseableIterator;
import org.apache.druid.metadata.IndexerSqlMetadataStorageCoordinatorTestBase;
import org.apache.druid.metadata.MetadataStorageTablesConfig;
import org.apache.druid.metadata.SqlSegmentsMetadataQuery;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.segment.TestDataSource;
import org.apache.druid.segment.TestHelper;
import org.apache.druid.server.coordinator.CreateDataSegments;
import org.apache.druid.timeline.DataSegment;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
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
@OutputTimeUnit(TimeUnit.MILLISECONDS)
public class SqlSegmentsMetadataQueryBenchmark
{

  private static final DateTime JAN_1 = DateTimes.of("2025-01-01");
  private static final String V1 = JAN_1.toString();
  private static final List<DataSegment> WIKI_SEGMENTS_1000X100D
      = CreateDataSegments.ofDatasource(TestDataSource.WIKI)
                          .forIntervals(100, Granularities.DAY)
                          .withNumPartitions(1000)
                          .startingAt(JAN_1)
                          .withVersion(V1)
                          .eachOfSizeInMb(500);

  private TestDerbyConnector derbyConnector;

  @Setup(Level.Trial)
  public void setup() throws Exception
  {
    this.derbyConnector = new TestDerbyConnector("druidBench");
    derbyConnector.getDBI().open().close();
    derbyConnector.createSegmentTable();
    insertSegments(WIKI_SEGMENTS_1000X100D.toArray(new DataSegment[0]));
  }

  @TearDown(Level.Trial)
  public void tearDown() throws Exception
  {
    derbyConnector.tearDown();
  }

  @Benchmark
  public void benchmarkRetrieveUsedSegments_returnAllSegments(Blackhole blackhole)
  {
    final Interval queryInterval = new Interval("2025-01-01T00:00:00.000Z/2025-04-11T00:00:00.000Z");
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval))));
  }

  @Benchmark
  public void benchmarkRetrieveUsedSegments_returnEmpty(Blackhole blackhole)
  {
    final Interval queryInterval = new Interval("2025-12-30T00:00:00.000Z/2025-12-31T00:00:00.000Z");
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval))));
  }

  @Benchmark
  public void benchmarkRetrieveUsedSegments_returnFirstInterval(Blackhole blackhole)
  {
    final Interval queryInterval = new Interval("2025-01-01T00:00:00.000Z/2025-01-02T00:00:00.000Z");
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval))));
  }

  @Benchmark
  public void benchmarkRetrieveUsedSegments_returnLastInterval(Blackhole blackhole)
  {
    final Interval queryInterval = new Interval("2025-04-10T00:00:00.000Z/2025-04-11T00:00:00.000Z");
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, List.of(queryInterval))));
  }


  @Benchmark
  public void benchmarkRetrieveUsedSegments_multipleIntervalsWithOverlaps(Blackhole blackhole)
  {
    List<Interval> intervals = List.of(
        new Interval("2025-01-01T00:00:00.000Z/2025-01-04T00:00:00.000Z"),
        new Interval("2025-01-03T00:00:00.000Z/2025-01-18T00:00:00.000Z"),
        new Interval("2025-02-01T00:00:00.000Z/2025-02-06T00:00:00.000Z"),
        new Interval("2025-02-05T00:00:00.000Z/2025-02-24T00:00:00.000Z"),
        new Interval("2025-03-10T00:00:00.000Z/2025-04-09T00:00:00.000Z")
    );
    blackhole.consume(readAsSet(q -> q.retrieveUsedSegments(TestDataSource.WIKI, intervals)));
  }

  private <T> Set<T> readAsSet(Function<SqlSegmentsMetadataQuery, CloseableIterator<T>> iterableReader)
  {
    final MetadataStorageTablesConfig tablesConfig = derbyConnector.getMetadataTablesConfigSupplier().get();

    return derbyConnector.inReadOnlyTransaction((handle, status) -> {
      final SqlSegmentsMetadataQuery query =
          SqlSegmentsMetadataQuery.forHandle(handle, derbyConnector, tablesConfig, TestHelper.JSON_MAPPER);

      try (CloseableIterator<T> iterator = iterableReader.apply(query)) {
        return ImmutableSet.copyOf(iterator);
      }
    });
  }

  private void insertSegments(DataSegment... segments)
  {
    IndexerSqlMetadataStorageCoordinatorTestBase.insertUsedSegments(
        Set.of(segments),
        Map.of(),
        derbyConnector,
        TestHelper.JSON_MAPPER
    );
  }
}
