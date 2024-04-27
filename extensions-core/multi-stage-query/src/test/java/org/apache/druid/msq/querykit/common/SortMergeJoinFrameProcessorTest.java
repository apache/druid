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

package org.apache.druid.msq.querykit.common;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.druid.common.config.NullHandling;
import org.apache.druid.common.guava.FutureUtils;
import org.apache.druid.frame.Frame;
import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.allocation.SingleMemoryAllocatorFactory;
import org.apache.druid.frame.channel.BlockingQueueFrameChannel;
import org.apache.druid.frame.channel.ReadableFrameChannel;
import org.apache.druid.frame.channel.ReadableNilFrameChannel;
import org.apache.druid.frame.key.KeyColumn;
import org.apache.druid.frame.key.KeyOrder;
import org.apache.druid.frame.processor.FrameProcessorExecutor;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.segment.FrameStorageAdapter;
import org.apache.druid.frame.testutil.FrameSequenceBuilder;
import org.apache.druid.frame.testutil.FrameTestUtil;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.Unit;
import org.apache.druid.java.util.common.concurrent.Execs;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Sequences;
import org.apache.druid.msq.indexing.error.MSQException;
import org.apache.druid.msq.indexing.error.TooManyRowsWithSameKeyFault;
import org.apache.druid.msq.input.ReadableInput;
import org.apache.druid.msq.kernel.StageId;
import org.apache.druid.msq.kernel.StagePartition;
import org.apache.druid.msq.test.LimitedFrameWriterFactory;
import org.apache.druid.segment.RowBasedSegment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.join.JoinTestHelper;
import org.apache.druid.segment.join.JoinType;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.apache.druid.timeline.SegmentId;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class SortMergeJoinFrameProcessorTest extends InitializedNullHandlingTest
{
  private static final StagePartition STAGE_PARTITION = new StagePartition(new StageId("q", 0), 0);
  private static final long MAX_BUFFERED_BYTES = 10_000_000;

  private final int rowsPerInputFrame;
  private final int rowsPerOutputFrame;

  private FrameProcessorExecutor exec;

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  public SortMergeJoinFrameProcessorTest(int rowsPerInputFrame, int rowsPerOutputFrame)
  {
    this.rowsPerInputFrame = rowsPerInputFrame;
    this.rowsPerOutputFrame = rowsPerOutputFrame;
  }

  @Parameterized.Parameters(name = "rowsPerInputFrame = {0}, rowsPerOutputFrame = {1}")
  public static Iterable<Object[]> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();

    for (final int rowsPerInputFrame : new int[]{1, 2, 7, Integer.MAX_VALUE}) {
      for (final int rowsPerOutputFrame : new int[]{1, 2, 7, Integer.MAX_VALUE}) {
        constructors.add(new Object[]{rowsPerInputFrame, rowsPerOutputFrame});
      }
    }

    return constructors;
  }

  @Before
  public void setUp()
  {
    exec = new FrameProcessorExecutor(MoreExecutors.listeningDecorator(Execs.singleThreaded("test-exec")));
  }

  @After
  public void tearDown() throws Exception
  {
    exec.getExecutorService().shutdownNow();
    exec.getExecutorService().awaitTermination(10, TimeUnit.MINUTES);
  }

  @Test
  public void testLeftJoinEmptyLeftSide() throws Exception
  {
    final ReadableInput factChannel = ReadableInput.channel(
        ReadableNilFrameChannel.INSTANCE,
        FrameReader.create(JoinTestHelper.FACT_SIGNATURE),
        STAGE_PARTITION
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.LEFT,
        MAX_BUFFERED_BYTES
    );

    assertResult(processor, outputChannel.readable(), joinSignature, Collections.emptyList());
  }

  @Test
  public void testLeftJoinEmptyRightSide() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );


    final ReadableInput countriesChannel = ReadableInput.channel(
        ReadableNilFrameChannel.INSTANCE,
        FrameReader.create(JoinTestHelper.COUNTRIES_SIGNATURE),
        STAGE_PARTITION
    );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.LEFT,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", null, null, null, null),
        Arrays.asList("Apamea abruzzorum", null, null, null, null),
        Arrays.asList("Atractus flammigerus", null, null, null, null),
        Arrays.asList("Rallicula", null, null, null, null),
        Arrays.asList("Talk:Oswald Tilghman", null, null, null, null),
        Arrays.asList("Peremptory norm", "AU", null, null, null),
        Arrays.asList("Didier Leclair", "CA", null, null, null),
        Arrays.asList("Les Argonautes", "CA", null, null, null),
        Arrays.asList("Sarah Michelle Gellar", "CA", null, null, null),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", null, null, null),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", null, null, null),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", null, null, null),
        Arrays.asList("Saison 9 de Secret Story", "FR", null, null, null),
        Arrays.asList("Glasgow", "GB", null, null, null),
        Arrays.asList("Giusy Ferreri discography", "IT", null, null, null),
        Arrays.asList("Roma-Bangkok", "IT", null, null, null),
        Arrays.asList("青野武", "JP", null, null, null),
        Arrays.asList("유희왕 GX", "KR", null, null, null),
        Arrays.asList("History of Fourems", "MMMM", null, null, null),
        Arrays.asList("Mathis Bolly", "MX", null, null, null),
        Arrays.asList("Orange Soda", "MatchNothing", null, null, null),
        Arrays.asList("Алиса в Зазеркалье", "NO", null, null, null),
        Arrays.asList("Cream Soda", "SU", null, null, null),
        Arrays.asList("Wendigo", "SV", null, null, null),
        Arrays.asList("Carlo Curti", "US", null, null, null),
        Arrays.asList("DirecTV", "US", null, null, null),
        Arrays.asList("Old Anatolian Turkish", "US", null, null, null),
        Arrays.asList("Otjiwarongo Airport", "US", null, null, null),
        Arrays.asList("President of India", "US", null, null, null)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinEmptyRightSide() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel = ReadableInput.channel(
        ReadableNilFrameChannel.INSTANCE,
        FrameReader.create(JoinTestHelper.COUNTRIES_SIGNATURE),
        STAGE_PARTITION
    );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    assertResult(processor, outputChannel.readable(), joinSignature, Collections.emptyList());
  }

  @Test
  public void testLeftJoinCountryIsoCode() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.LEFT,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", null, null, null, null),
        Arrays.asList("Apamea abruzzorum", null, null, null, null),
        Arrays.asList("Atractus flammigerus", null, null, null, null),
        Arrays.asList("Rallicula", null, null, null, null),
        Arrays.asList("Talk:Oswald Tilghman", null, null, null, null),
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Orange Soda", "MatchNothing", null, null, null),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testCrossJoin() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel = makeChannelFromResourceWithLimit(
        JoinTestHelper.COUNTRIES_RESOURCE,
        JoinTestHelper.COUNTRIES_SIGNATURE,
        ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
        2
    );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        countriesChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(Collections.emptyList(), Collections.emptyList()),
        new int[0],
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", "AU"),
        Arrays.asList("Agama mossambica", "CA"),
        Arrays.asList("Apamea abruzzorum", "AU"),
        Arrays.asList("Apamea abruzzorum", "CA"),
        Arrays.asList("Atractus flammigerus", "AU"),
        Arrays.asList("Atractus flammigerus", "CA"),
        Arrays.asList("Rallicula", "AU"),
        Arrays.asList("Rallicula", "CA"),
        Arrays.asList("Talk:Oswald Tilghman", "AU"),
        Arrays.asList("Talk:Oswald Tilghman", "CA"),
        Arrays.asList("Peremptory norm", "AU"),
        Arrays.asList("Peremptory norm", "CA"),
        Arrays.asList("Didier Leclair", "AU"),
        Arrays.asList("Didier Leclair", "CA"),
        Arrays.asList("Les Argonautes", "AU"),
        Arrays.asList("Les Argonautes", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "AU"),
        Arrays.asList("Sarah Michelle Gellar", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "AU"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CA"),
        Arrays.asList("Diskussion:Sebastian Schulz", "AU"),
        Arrays.asList("Diskussion:Sebastian Schulz", "CA"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "AU"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "CA"),
        Arrays.asList("Saison 9 de Secret Story", "AU"),
        Arrays.asList("Saison 9 de Secret Story", "CA"),
        Arrays.asList("Glasgow", "AU"),
        Arrays.asList("Glasgow", "CA"),
        Arrays.asList("Giusy Ferreri discography", "AU"),
        Arrays.asList("Giusy Ferreri discography", "CA"),
        Arrays.asList("Roma-Bangkok", "AU"),
        Arrays.asList("Roma-Bangkok", "CA"),
        Arrays.asList("青野武", "AU"),
        Arrays.asList("青野武", "CA"),
        Arrays.asList("유희왕 GX", "AU"),
        Arrays.asList("유희왕 GX", "CA"),
        Arrays.asList("History of Fourems", "AU"),
        Arrays.asList("History of Fourems", "CA"),
        Arrays.asList("Mathis Bolly", "AU"),
        Arrays.asList("Mathis Bolly", "CA"),
        Arrays.asList("Orange Soda", "AU"),
        Arrays.asList("Orange Soda", "CA"),
        Arrays.asList("Алиса в Зазеркалье", "AU"),
        Arrays.asList("Алиса в Зазеркалье", "CA"),
        Arrays.asList("Cream Soda", "AU"),
        Arrays.asList("Cream Soda", "CA"),
        Arrays.asList("Wendigo", "AU"),
        Arrays.asList("Wendigo", "CA"),
        Arrays.asList("Carlo Curti", "AU"),
        Arrays.asList("Carlo Curti", "CA"),
        Arrays.asList("DirecTV", "AU"),
        Arrays.asList("DirecTV", "CA"),
        Arrays.asList("Old Anatolian Turkish", "AU"),
        Arrays.asList("Old Anatolian Turkish", "CA"),
        Arrays.asList("Otjiwarongo Airport", "AU"),
        Arrays.asList("Otjiwarongo Airport", "CA"),
        Arrays.asList("President of India", "AU"),
        Arrays.asList("President of India", "CA")
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testLeftJoinRegions() throws Exception
  {
    final ReadableInput factChannel =
        buildFactInput(
            ImmutableList.of(
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("page", KeyOrder.ASCENDING)
            )
        );

    final ReadableInput regionsChannel =
        buildRegionsInput(
            ImmutableList.of(
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)
            )
        );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("j0.regionName", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        regionsChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)
            ),
            ImmutableList.of(
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)
            )
        ),
        new int[]{0, 1},
        JoinType.LEFT,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", null, null),
        Arrays.asList("Apamea abruzzorum", null, null),
        Arrays.asList("Atractus flammigerus", null, null),
        Arrays.asList("Rallicula", null, null),
        Arrays.asList("Talk:Oswald Tilghman", null, null),
        Arrays.asList("Peremptory norm", "New South Wales", "AU"),
        Arrays.asList("Didier Leclair", "Ontario", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "Ontario", "CA"),
        Arrays.asList("Les Argonautes", "Quebec", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "CL"),
        Arrays.asList("Diskussion:Sebastian Schulz", "Hesse", "DE"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "EC"),
        Arrays.asList("Saison 9 de Secret Story", "Val d'Oise", "FR"),
        Arrays.asList("Glasgow", "Kingston upon Hull", "GB"),
        Arrays.asList("Giusy Ferreri discography", "Provincia di Varese", "IT"),
        Arrays.asList("Roma-Bangkok", "Provincia di Varese", "IT"),
        Arrays.asList("青野武", "Tōkyō", "JP"),
        Arrays.asList("유희왕 GX", "Seoul", "KR"),
        Arrays.asList("History of Fourems", "Fourems Province", "MMMM"),
        Arrays.asList("Mathis Bolly", "Mexico City", "MX"),
        Arrays.asList("Orange Soda", null, "MatchNothing"),
        Arrays.asList("Алиса в Зазеркалье", "Finnmark Fylke", "NO"),
        Arrays.asList("Cream Soda", "Ainigriv", "SU"),
        Arrays.asList("Wendigo", "Departamento de San Salvador", "SV"),
        Arrays.asList("Carlo Curti", "California", "US"),
        Arrays.asList("Otjiwarongo Airport", "California", "US"),
        Arrays.asList("President of India", "California", "US"),
        Arrays.asList("DirecTV", "North Carolina", "US"),
        Arrays.asList("Old Anatolian Turkish", "Virginia", "US")
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testRightJoinRegionCodeOnly() throws Exception
  {
    // This join generates duplicates.

    final ReadableInput factChannel =
        buildFactInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("page", KeyOrder.ASCENDING)
            )
        );

    final ReadableInput regionsChannel =
        buildRegionsInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)
            )
        );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("regionName", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        regionsChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.RIGHT,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", null, null),
        Arrays.asList("Apamea abruzzorum", null, null),
        Arrays.asList("Atractus flammigerus", null, null),
        Arrays.asList("Rallicula", null, null),
        Arrays.asList("Talk:Oswald Tilghman", null, null),
        Arrays.asList("유희왕 GX", "Seoul", "KR"),
        Arrays.asList("青野武", "Tōkyō", "JP"),
        Arrays.asList("Алиса в Зазеркалье", "Finnmark Fylke", "NO"),
        Arrays.asList("Saison 9 de Secret Story", "Val d'Oise", "FR"),
        Arrays.asList("Cream Soda", "Ainigriv", "SU"),
        Arrays.asList("Carlo Curti", "California", "US"),
        Arrays.asList("Otjiwarongo Airport", "California", "US"),
        Arrays.asList("President of India", "California", "US"),
        Arrays.asList("Mathis Bolly", "Mexico City", "MX"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "EC"),
        Arrays.asList("Diskussion:Sebastian Schulz", "Hesse", "DE"),
        Arrays.asList("Glasgow", "Kingston upon Hull", "GB"),
        Arrays.asList("History of Fourems", "Fourems Province", "MMMM"),
        Arrays.asList("Orange Soda", null, "MatchNothing"),
        Arrays.asList("DirecTV", "North Carolina", "US"),
        Arrays.asList("Peremptory norm", "New South Wales", "AU"),
        Arrays.asList("Didier Leclair", "Ontario", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "Ontario", "CA"),
        Arrays.asList("Les Argonautes", "Quebec", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "CL"),
        Arrays.asList("Wendigo", "Departamento de San Salvador", "SV"),
        Arrays.asList("Giusy Ferreri discography", "Provincia di Varese", "IT"),
        Arrays.asList("Giusy Ferreri discography", "Virginia", "IT"),
        Arrays.asList("Old Anatolian Turkish", "Provincia di Varese", "US"),
        Arrays.asList("Old Anatolian Turkish", "Virginia", "US"),
        Arrays.asList("Roma-Bangkok", "Provincia di Varese", "IT"),
        Arrays.asList("Roma-Bangkok", "Virginia", "IT")
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testFullOuterJoinRegionCodeOnly() throws Exception
  {
    // This join generates duplicates.

    final ReadableInput factChannel =
        buildFactInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("page", KeyOrder.ASCENDING)
            )
        );

    final ReadableInput regionsChannel =
        buildRegionsInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)
            )
        );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("regionName", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        regionsChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.FULL,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList(null, "Nulland", null),
        Arrays.asList("Agama mossambica", null, null),
        Arrays.asList("Apamea abruzzorum", null, null),
        Arrays.asList("Atractus flammigerus", null, null),
        Arrays.asList("Rallicula", null, null),
        Arrays.asList("Talk:Oswald Tilghman", null, null),
        Arrays.asList("유희왕 GX", "Seoul", "KR"),
        Arrays.asList("青野武", "Tōkyō", "JP"),
        Arrays.asList("Алиса в Зазеркалье", "Finnmark Fylke", "NO"),
        Arrays.asList("Saison 9 de Secret Story", "Val d'Oise", "FR"),
        Arrays.asList(null, "Foureis Province", null),
        Arrays.asList("Cream Soda", "Ainigriv", "SU"),
        Arrays.asList("Carlo Curti", "California", "US"),
        Arrays.asList("Otjiwarongo Airport", "California", "US"),
        Arrays.asList("President of India", "California", "US"),
        Arrays.asList("Mathis Bolly", "Mexico City", "MX"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "EC"),
        Arrays.asList("Diskussion:Sebastian Schulz", "Hesse", "DE"),
        Arrays.asList("Glasgow", "Kingston upon Hull", "GB"),
        Arrays.asList("History of Fourems", "Fourems Province", "MMMM"),
        Arrays.asList("Orange Soda", null, "MatchNothing"),
        Arrays.asList("DirecTV", "North Carolina", "US"),
        Arrays.asList("Peremptory norm", "New South Wales", "AU"),
        Arrays.asList("Didier Leclair", "Ontario", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "Ontario", "CA"),
        Arrays.asList("Les Argonautes", "Quebec", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "CL"),
        Arrays.asList("Wendigo", "Departamento de San Salvador", "SV"),
        Arrays.asList("Giusy Ferreri discography", "Provincia di Varese", "IT"),
        Arrays.asList("Giusy Ferreri discography", "Virginia", "IT"),
        Arrays.asList("Old Anatolian Turkish", "Provincia di Varese", "US"),
        Arrays.asList("Old Anatolian Turkish", "Virginia", "US"),
        Arrays.asList("Roma-Bangkok", "Provincia di Varese", "IT"),
        Arrays.asList("Roma-Bangkok", "Virginia", "IT"),
        Arrays.asList(null, "Usca City", null)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinRegionCodeOnly() throws Exception
  {
    // This join generates duplicates.

    final ReadableInput factChannel =
        buildFactInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("page", KeyOrder.ASCENDING)
            )
        );

    final ReadableInput regionsChannel =
        buildRegionsInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)
            )
        );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("regionName", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        regionsChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("유희왕 GX", "Seoul", "KR"),
        Arrays.asList("青野武", "Tōkyō", "JP"),
        Arrays.asList("Алиса в Зазеркалье", "Finnmark Fylke", "NO"),
        Arrays.asList("Saison 9 de Secret Story", "Val d'Oise", "FR"),
        Arrays.asList("Cream Soda", "Ainigriv", "SU"),
        Arrays.asList("Carlo Curti", "California", "US"),
        Arrays.asList("Otjiwarongo Airport", "California", "US"),
        Arrays.asList("President of India", "California", "US"),
        Arrays.asList("Mathis Bolly", "Mexico City", "MX"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "EC"),
        Arrays.asList("Diskussion:Sebastian Schulz", "Hesse", "DE"),
        Arrays.asList("Glasgow", "Kingston upon Hull", "GB"),
        Arrays.asList("History of Fourems", "Fourems Province", "MMMM"),
        Arrays.asList("DirecTV", "North Carolina", "US"),
        Arrays.asList("Peremptory norm", "New South Wales", "AU"),
        Arrays.asList("Didier Leclair", "Ontario", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "Ontario", "CA"),
        Arrays.asList("Les Argonautes", "Quebec", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "CL"),
        Arrays.asList("Wendigo", "Departamento de San Salvador", "SV"),
        Arrays.asList("Giusy Ferreri discography", "Provincia di Varese", "IT"),
        Arrays.asList("Giusy Ferreri discography", "Virginia", "IT"),
        Arrays.asList("Old Anatolian Turkish", "Provincia di Varese", "US"),
        Arrays.asList("Old Anatolian Turkish", "Virginia", "US"),
        Arrays.asList("Roma-Bangkok", "Provincia di Varese", "IT"),
        Arrays.asList("Roma-Bangkok", "Virginia", "IT")
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinRegionCodeOnlyIsNotDistinctFrom() throws Exception
  {
    // This join generates duplicates.

    final ReadableInput factChannel =
        buildFactInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("page", KeyOrder.ASCENDING)
            )
        );

    final ReadableInput regionsChannel =
        buildRegionsInput(
            ImmutableList.of(
                new KeyColumn("regionIsoCode", KeyOrder.ASCENDING),
                new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)
            )
        );

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("regionName", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        regionsChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("regionIsoCode", KeyOrder.ASCENDING))
        ),
        new int[0], // empty array: act as if IS NOT DISTINCT FROM
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Agama mossambica", "Nulland", null),
        Arrays.asList("Apamea abruzzorum", "Nulland", null),
        Arrays.asList("Atractus flammigerus", "Nulland", null),
        Arrays.asList("Rallicula", "Nulland", null),
        Arrays.asList("Talk:Oswald Tilghman", "Nulland", null),
        Arrays.asList("유희왕 GX", "Seoul", "KR"),
        Arrays.asList("青野武", "Tōkyō", "JP"),
        Arrays.asList("Алиса в Зазеркалье", "Finnmark Fylke", "NO"),
        Arrays.asList("Saison 9 de Secret Story", "Val d'Oise", "FR"),
        Arrays.asList("Cream Soda", "Ainigriv", "SU"),
        Arrays.asList("Carlo Curti", "California", "US"),
        Arrays.asList("Otjiwarongo Airport", "California", "US"),
        Arrays.asList("President of India", "California", "US"),
        Arrays.asList("Mathis Bolly", "Mexico City", "MX"),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "Provincia del Guayas", "EC"),
        Arrays.asList("Diskussion:Sebastian Schulz", "Hesse", "DE"),
        Arrays.asList("Glasgow", "Kingston upon Hull", "GB"),
        Arrays.asList("History of Fourems", "Fourems Province", "MMMM"),
        Arrays.asList("DirecTV", "North Carolina", "US"),
        Arrays.asList("Peremptory norm", "New South Wales", "AU"),
        Arrays.asList("Didier Leclair", "Ontario", "CA"),
        Arrays.asList("Sarah Michelle Gellar", "Ontario", "CA"),
        Arrays.asList("Les Argonautes", "Quebec", "CA"),
        Arrays.asList("Golpe de Estado en Chile de 1973", "Santiago Metropolitan", "CL"),
        Arrays.asList("Wendigo", "Departamento de San Salvador", "SV"),
        Arrays.asList("Giusy Ferreri discography", "Provincia di Varese", "IT"),
        Arrays.asList("Giusy Ferreri discography", "Virginia", "IT"),
        Arrays.asList("Old Anatolian Turkish", "Provincia di Varese", "US"),
        Arrays.asList("Old Anatolian Turkish", "Virginia", "US"),
        Arrays.asList("Roma-Bangkok", "Provincia di Varese", "IT"),
        Arrays.asList("Roma-Bangkok", "Virginia", "IT")
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testLeftJoinCountryNumber() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryNumber", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.LEFT,
        MAX_BUFFERED_BYTES
    );

    final String countryCodeForNull;
    final String countryNameForNull;
    final Long countryNumberForNull;

    if (NullHandling.sqlCompatible()) {
      countryCodeForNull = null;
      countryNameForNull = null;
      countryNumberForNull = null;
    } else {
      // In default-value mode, null country number from the left-hand table converts to zero, which matches Australia.
      countryCodeForNull = "AU";
      countryNameForNull = "Australia";
      countryNumberForNull = 0L;
    }

    final List<List<Object>> expectedRows = Lists.newArrayList(
        Arrays.asList("Agama mossambica", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Apamea abruzzorum", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Atractus flammigerus", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Rallicula", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Talk:Oswald Tilghman", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Orange Soda", "MatchNothing", null, null, null),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L)
    );

    if (!NullHandling.sqlCompatible()) {
      // Sorting order is different in default-value mode, since 0 and null collapse.
      // "Peremptory norm" moves before "Rallicula".
      expectedRows.add(3, expectedRows.remove(5));
    }

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testRightJoinCountryNumber() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryNumber", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("countryName", ColumnType.STRING)
                    .add("countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        countriesChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryNumber", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.RIGHT,
        MAX_BUFFERED_BYTES
    );

    final String countryCodeForNull;
    final String countryNameForNull;
    final Long countryNumberForNull;

    if (NullHandling.sqlCompatible()) {
      countryCodeForNull = null;
      countryNameForNull = null;
      countryNumberForNull = null;
    } else {
      // In default-value mode, null country number from the left-hand table converts to zero, which matches Australia.
      countryCodeForNull = "AU";
      countryNameForNull = "Australia";
      countryNumberForNull = 0L;
    }

    final List<List<Object>> expectedRows = Lists.newArrayList(
        Arrays.asList("Agama mossambica", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Apamea abruzzorum", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Atractus flammigerus", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Rallicula", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Talk:Oswald Tilghman", null, countryCodeForNull, countryNameForNull, countryNumberForNull),
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Orange Soda", "MatchNothing", null, null, null),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L)
    );

    if (!NullHandling.sqlCompatible()) {
      // Sorting order is different in default-value mode, since 0 and null collapse.
      // "Peremptory norm" moves before "Rallicula".
      expectedRows.add(3, expectedRows.remove(5));
    }

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinCountryIsoCode() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinCountryIsoCodeNotDistinctFrom() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[0], // empty array: act as if IS NOT DISTINCT FROM
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinCountryIsoCode_withMaxBufferedBytesLimit_succeeds() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("page", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("j0.countryName", ColumnType.STRING)
                    .add("j0.countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel,
        countriesChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        1
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testInnerJoinCountryIsoCode_backwards_withMaxBufferedBytesLimit_succeeds() throws Exception
  {
    final ReadableInput factChannel = buildFactInput(
        ImmutableList.of(
            new KeyColumn("countryIsoCode", KeyOrder.ASCENDING),
            new KeyColumn("page", KeyOrder.ASCENDING)
        )
    );

    final ReadableInput countriesChannel =
        buildCountriesInput(ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("j0.page", ColumnType.STRING)
                    .add("j0.countryIsoCode", ColumnType.STRING)
                    .add("countryIsoCode", ColumnType.STRING)
                    .add("countryName", ColumnType.STRING)
                    .add("countryNumber", ColumnType.LONG)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        countriesChannel,
        factChannel,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("countryIsoCode", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        1
    );

    final List<List<Object>> expectedRows = Arrays.asList(
        Arrays.asList("Peremptory norm", "AU", "AU", "Australia", 0L),
        Arrays.asList("Didier Leclair", "CA", "CA", "Canada", 1L),
        Arrays.asList("Les Argonautes", "CA", "CA", "Canada", 1L),
        Arrays.asList("Sarah Michelle Gellar", "CA", "CA", "Canada", 1L),
        Arrays.asList("Golpe de Estado en Chile de 1973", "CL", "CL", "Chile", 2L),
        Arrays.asList("Diskussion:Sebastian Schulz", "DE", "DE", "Germany", 3L),
        Arrays.asList("Gabinete Ministerial de Rafael Correa", "EC", "EC", "Ecuador", 4L),
        Arrays.asList("Saison 9 de Secret Story", "FR", "FR", "France", 5L),
        Arrays.asList("Glasgow", "GB", "GB", "United Kingdom", 6L),
        Arrays.asList("Giusy Ferreri discography", "IT", "IT", "Italy", 7L),
        Arrays.asList("Roma-Bangkok", "IT", "IT", "Italy", 7L),
        Arrays.asList("青野武", "JP", "JP", "Japan", 8L),
        Arrays.asList("유희왕 GX", "KR", "KR", "Republic of Korea", 9L),
        Arrays.asList("History of Fourems", "MMMM", "MMMM", "Fourems", 205L),
        Arrays.asList("Mathis Bolly", "MX", "MX", "Mexico", 10L),
        Arrays.asList("Алиса в Зазеркалье", "NO", "NO", "Norway", 11L),
        Arrays.asList("Cream Soda", "SU", "SU", "States United", 15L),
        Arrays.asList("Wendigo", "SV", "SV", "El Salvador", 12L),
        Arrays.asList("Carlo Curti", "US", "US", "United States", 13L),
        Arrays.asList("DirecTV", "US", "US", "United States", 13L),
        Arrays.asList("Old Anatolian Turkish", "US", "US", "United States", 13L),
        Arrays.asList("Otjiwarongo Airport", "US", "US", "United States", 13L),
        Arrays.asList("President of India", "US", "US", "United States", 13L)
    );

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testCountrySelfJoin() throws Exception
  {
    final ReadableInput factChannel1 = buildFactInput(ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)));
    final ReadableInput factChannel2 = buildFactInput(ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("channel", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel1,
        factChannel2,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        MAX_BUFFERED_BYTES
    );

    final List<List<Object>> expectedRows = new ArrayList<>();

    final ImmutableMap<String, Long> expectedCounts =
        ImmutableMap.<String, Long>builder()
                    .put("#ca.wikipedia", 1L)
                    .put("#de.wikipedia", 1L)
                    .put("#en.wikipedia", 196L)
                    .put("#es.wikipedia", 16L)
                    .put("#fr.wikipedia", 9L)
                    .put("#ja.wikipedia", 1L)
                    .put("#ko.wikipedia", 1L)
                    .put("#ru.wikipedia", 1L)
                    .put("#vi.wikipedia", 9L)
                    .build();

    for (final Map.Entry<String, Long> entry : expectedCounts.entrySet()) {
      for (int i = 0; i < Ints.checkedCast(entry.getValue()); i++) {
        expectedRows.add(Collections.singletonList(entry.getKey()));
      }
    }

    assertResult(processor, outputChannel.readable(), joinSignature, expectedRows);
  }

  @Test
  public void testCountrySelfJoin_withMaxBufferedBytesLimit_fails() throws Exception
  {
    // Test is only valid when rowsPerInputFrame is low enough that we get multiple frames.
    Assume.assumeThat(rowsPerInputFrame, Matchers.lessThanOrEqualTo(7));

    final ReadableInput factChannel1 = buildFactInput(ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)));
    final ReadableInput factChannel2 = buildFactInput(ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)));

    final BlockingQueueFrameChannel outputChannel = BlockingQueueFrameChannel.minimal();

    final RowSignature joinSignature =
        RowSignature.builder()
                    .add("channel", ColumnType.STRING)
                    .build();

    final SortMergeJoinFrameProcessor processor = new SortMergeJoinFrameProcessor(
        factChannel1,
        factChannel2,
        outputChannel.writable(),
        makeFrameWriterFactory(joinSignature),
        "j0.",
        ImmutableList.of(
            ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING)),
            ImmutableList.of(new KeyColumn("channel", KeyOrder.ASCENDING))
        ),
        new int[]{0},
        JoinType.INNER,
        1
    );

    final RuntimeException e = Assert.assertThrows(
        RuntimeException.class,
        () -> run(processor, outputChannel.readable(), joinSignature)
    );

    MatcherAssert.assertThat(e.getCause(), CoreMatchers.instanceOf(RuntimeException.class));
    MatcherAssert.assertThat(e.getCause().getCause(), CoreMatchers.instanceOf(MSQException.class));
    MatcherAssert.assertThat(
        ((MSQException) e.getCause().getCause()).getFault(),
        CoreMatchers.instanceOf(TooManyRowsWithSameKeyFault.class)
    );
  }

  private void assertResult(
      final SortMergeJoinFrameProcessor processor,
      final ReadableFrameChannel readableOutputChannel,
      final RowSignature joinSignature,
      final List<List<Object>> expectedRows
  )
  {
    final List<List<Object>> rowsFromProcessor = run(processor, readableOutputChannel, joinSignature);
    FrameTestUtil.assertRowsEqual(Sequences.simple(expectedRows), Sequences.simple(rowsFromProcessor));
  }

  private List<List<Object>> run(
      final SortMergeJoinFrameProcessor processor,
      final ReadableFrameChannel readableOutputChannel,
      final RowSignature joinSignature
  )
  {
    final ListenableFuture<Object> retValFromProcessor = exec.runFully(processor, null);
    final Sequence<List<Object>> rowsFromProcessor = FrameTestUtil.readRowsFromFrameChannel(
        readableOutputChannel,
        FrameReader.create(joinSignature)
    );

    final List<List<Object>> rows = rowsFromProcessor.toList();
    Assert.assertEquals(Unit.instance(), FutureUtils.getUnchecked(retValFromProcessor, true));
    return rows;
  }

  private ReadableInput buildFactInput(final List<KeyColumn> keyColumns) throws IOException
  {
    return makeChannelFromResource(
        JoinTestHelper.FACT_RESOURCE,
        JoinTestHelper.FACT_SIGNATURE,
        keyColumns
    );
  }

  private ReadableInput buildCountriesInput(final List<KeyColumn> keyColumns) throws IOException
  {
    return makeChannelFromResource(
        JoinTestHelper.COUNTRIES_RESOURCE,
        JoinTestHelper.COUNTRIES_SIGNATURE,
        keyColumns
    );
  }

  private ReadableInput buildRegionsInput(final List<KeyColumn> keyColumns) throws IOException
  {
    return makeChannelFromResource(
        JoinTestHelper.REGIONS_RESOURCE,
        JoinTestHelper.REGIONS_SIGNATURE,
        keyColumns
    );
  }

  private ReadableInput makeChannelFromResource(
      final String resource,
      final RowSignature signature,
      final List<KeyColumn> keyColumns
  ) throws IOException
  {
    return makeChannelFromResourceWithLimit(resource, signature, keyColumns, -1);
  }

  private ReadableInput makeChannelFromResourceWithLimit(
      final String resource,
      final RowSignature signature,
      final List<KeyColumn> keyColumns,
      final long limit
  ) throws IOException
  {
    try (final RowBasedSegment<Map<String, Object>> segment = JoinTestHelper.withRowsFromResource(
        resource,
        rows -> new RowBasedSegment<>(
            SegmentId.dummy(resource),
            limit < 0 ? Sequences.simple(rows) : Sequences.simple(rows).limit(limit),
            columnName -> m -> m.get(columnName),
            signature
        )
    )) {
      final StorageAdapter adapter = segment.asStorageAdapter();
      return makeChannelFromAdapter(adapter, keyColumns);
    }
  }

  private ReadableInput makeChannelFromAdapter(
      final StorageAdapter adapter,
      final List<KeyColumn> keyColumns
  ) throws IOException
  {
    // Create a single, sorted frame.
    final FrameSequenceBuilder singleFrameBuilder =
        FrameSequenceBuilder.fromAdapter(adapter)
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(Integer.MAX_VALUE)
                            .sortBy(keyColumns);

    final RowSignature signature = singleFrameBuilder.signature();
    final Frame frame = Iterables.getOnlyElement(singleFrameBuilder.frames().toList());

    // Split it up into frames that match rowsPerFrame. Set max size enough to hold all rows we might ever want to use.
    final BlockingQueueFrameChannel channel = new BlockingQueueFrameChannel(10_000);

    final FrameReader frameReader = FrameReader.create(signature);

    final FrameSequenceBuilder frameSequenceBuilder =
        FrameSequenceBuilder.fromAdapter(new FrameStorageAdapter(frame, frameReader, Intervals.ETERNITY))
                            .frameType(FrameType.ROW_BASED)
                            .maxRowsPerFrame(rowsPerInputFrame);

    final Sequence<Frame> frames = frameSequenceBuilder.frames();
    frames.forEach(
        f -> {
          try {
            channel.writable().write(f);
          }
          catch (IOException e) {
            throw new RuntimeException(e);
          }
        }
    );

    channel.writable().close();
    return ReadableInput.channel(channel.readable(), FrameReader.create(signature), STAGE_PARTITION);
  }

  private FrameWriterFactory makeFrameWriterFactory(final RowSignature signature)
  {
    return new LimitedFrameWriterFactory(
        FrameWriters.makeFrameWriterFactory(
            FrameType.ROW_BASED,
            new SingleMemoryAllocatorFactory(ArenaMemoryAllocator.createOnHeap(1_000_000)),
            signature,
            Collections.emptyList()
        ),
        rowsPerOutputFrame
    );
  }
}
