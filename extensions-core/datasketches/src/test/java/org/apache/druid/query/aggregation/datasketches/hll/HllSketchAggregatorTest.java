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

package org.apache.druid.query.aggregation.datasketches.hll;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.StringEncoding;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.testing.InitializedNullHandlingTest;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@RunWith(Parameterized.class)
public class HllSketchAggregatorTest extends InitializedNullHandlingTest
{
  private static final boolean ROUND = true;

  private final AggregationTestHelper groupByHelper;
  private final AggregationTestHelper timeseriesHelper;
  private final QueryContexts.Vectorize vectorize;
  private final StringEncoding stringEncoding;

  @Rule
  public final TemporaryFolder groupByFolder = new TemporaryFolder();

  @Rule
  public final TemporaryFolder timeseriesFolder = new TemporaryFolder();

  public HllSketchAggregatorTest(GroupByQueryConfig config, String vectorize, StringEncoding stringEncoding)
  {
    HllSketchModule.registerSerde();
    groupByHelper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), config, groupByFolder
    );
    timeseriesHelper = AggregationTestHelper.createTimeseriesQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), timeseriesFolder
    );
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.stringEncoding = stringEncoding;
  }

  @Parameterized.Parameters(name = "groupByConfig = {0}, vectorize = {1}, stringEncoding = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "force"}) {
        for (StringEncoding stringEncoding : StringEncoding.values()) {
          if (!("v1".equals(config.getDefaultStrategy()) && "force".equals(vectorize))) {
            constructors.add(new Object[]{config, vectorize, stringEncoding});
          }
        }
      }
    }
    return constructors;
  }

  @Test
  public void ingestSketches() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
  }

  @Test
  public void ingestSketchesTimeseries() throws Exception
  {
    final File inputFile = new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile());
    final String parserJson = buildParserJson(
        Arrays.asList("dim", "multiDim"),
        Arrays.asList("timestamp", "dim", "multiDim", "sketch")
    );
    final String aggregators =
        buildAggregatorJson("HLLSketchMerge", "sketch", !ROUND, HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING);
    final int minTimestamp = 0;
    final Granularity gran = Granularities.NONE;
    final int maxRowCount = 10;
    final String queryJson = buildTimeseriesQueryJson("HLLSketchMerge", "sketch", !ROUND);

    File segmentDir1 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(inputFile, parserJson, aggregators, segmentDir1, minTimestamp, gran, maxRowCount, true);

    File segmentDir2 = timeseriesFolder.newFolder();
    timeseriesHelper.createIndex(inputFile, parserJson, aggregators, segmentDir2, minTimestamp, gran, maxRowCount, true);

    Sequence<Result> seq = timeseriesHelper.runQueryOnSegments(Arrays.asList(segmentDir1, segmentDir2), queryJson);
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Collections.singletonList("dim"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        buildAggregatorJson("HLLSketchBuild", "id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketch;

    if (stringEncoding == StringEncoding.UTF16LE) {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAcoDCQOUmAsElgIEcSjfBAmeJQYLkjgKDFoJBQ6yWQUP9hIPONtCBxPgngsW9l4IGfQIChp8/wQbxkIHHXaYC+0y0Q"
          + "eEPH8EJ3ATBSrq7wYrds8ELKKxBjH4RAszAMEFOJLSDzkQqgg6gNUEP0KEBwlswAVDtA0dRTzeBMx7tApHhJILSYB9CEpS4QVQcMsF3n"
          + "SbBFmoKhRbbokL+kJiCl44/AVlpqEEcer/Bmd8iQlofI0Ha7heB23oKARuPnMGcb7fB3VY4wh55qsHe/h1CHzYUwamr80Hf16DCYBwzg"
          + "i+g0wFhMhvBIsKgAiNhmIGjmBkB6/iiw/pUEcGhBAcBGZmVAeh9REFoCxhD6Es1Q+l4CwJqlRpBKsM1gfMxd0KtIwxCrYsHQ63UowVuQ"
          + "qjB73OIAy+OuwHweJIBwN+PQWWXBwLxubHCsnuvgfLEGcHzPAPB86SMgTLCPAIiUmHB9VePwbYkMUG3sKGCyK9lQoWlpkH5/BcB+m2wg"
          + "ftNIEGS0tPBviYvAv5SLUGtEUfBvtiMwX9viUE/h7SDgGNNQwCBQMFB6dcBPVhlwcJ77sLC+ebCw3dygoP3Z4FEMGyBxFLpAUf2bsHIu"
          + "3JBSXnTwUq3/4SK2mVBy5/7QQvcwIFMCk0CTLbWwQ0P+4ENkkTCjgt+Qw8tXoGQtEPE+lYCgdEqUwLxgTJDEv9gQdN4W4ETsFoCU/zNw"
          + "ZTpzEPVE3EBlXvPARXHaIEW3+oE1yZzAZhI7EEY583EWTztAVlcwoGZi0iGiKPGAS/Z6MFezmHBIFVHReGx8UIibNbBYEZzAiQowUIms"
          + "sVB5wTzQahxdAFpMWTDKWjjwims/4Fqbd7BaoRyh2tsZwLrwXmBLET4gu0azkFuAWtBry3Dge+PwUNvy9jBMCzegYEfLsGa7LSCsVzoA"
          + "rKO+IGzGUaC8/XMgTRD58H1YNvBtY1lwUaeD8FyRyaC+3c8Qfd44IT9U2hBOnuvQzjn2QHvifoBufnqw44YtMM6mGgB/7YVQXyGboGPG"
          + "+1E/SHpQr1M0UH4BX8BPkTeQX602UF/ZESEf/DRQs=";
    } else {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDgsQ4gcOMh8IEJhlExuaBgd+R1EH/lKSDH1cvxsv+IIEue3UBjIAygY0jNQENawDBotViQ"
          + "Y3iIcHO6wsBj80qwZE5r8GRuL2DEfMjAdJeDYHUXQ6BVWetwlWaGwLWNhTBFpAbQZeliYI8eOEBGiAJwVp1isGd94kCHgYHhGqpg8GwH"
          + "9VBoB4UAqcyxUQheCsGIciaw6JvKgGi1xBDI1i0gWPLNkOkBDgC5KA9AqTFiQIl8TGBplo/wSeZFwGow5EBeOCjw2mdoIEqaqsBarS1w"
          + "+u1ioPr7ywCbJsyge5+eEFuIw5BbncbgXaxlcEwk4JFcQy2QnHhJ4Uywa4DtEkEQbU9qIL2STtD9rcygTbYAUI2wIoBd1GHAbi8uYF48"
          + "BYEuZUJAxSK2sI6ZwIBusYkRTs1pQEuWlaCLk9MQX4wqoaAPb9CvyuJgf+SEwE/06oCQD7lwcBH8MGAgHuGHgVqw0Ic04JDoWnEg/H5Q"
          + "oRlZkVEm2xBhS/lQYa/b8THXcwBh9TsgUikxwQJZ1FHoifNwony8ILflUbBSppxwsrHTsELX0JBi6PYgkxL10GMl8nDioLmgXskNwFOf"
          + "WCCTyZYg0+8YgKQQMdDE3pbA5OGScOEIDyBFJJ9QdXBV4EW62RDmTjrwUA7pwVZwecDGnP9gZrVRsGbbOpEXHRsB52LxEGdxPyDnivTw"
          + "n+PBgEe02DBH3nEAR+vUoaf18LBlFkNgSFaRsKhvktCIj36guL8yAbj++8BpGfJwWTDXkblLFVBpdT5gWYB1IRmiNTD7XNXwqeA2UP6b"
          + "SaBTHddAbej8sEqzU0C6z5YA6vpRcKsve8Fzl5HAk88agZt6tFCFJn6hO5t5kFvN2cC71rfga/QaAOwL3zCsM59wbFv4gEmViuBciRjg"
          + "bJSXQMy9GbEOPSBQTNd2QEzzmRES+iLxLT8ZEF1K3oKNa36gTYC/0JDpijBN1h7AneDRAe35diD+ENjwnio30H5KVqFOjHFgbppaIS6x"
          + "dHBotkQRbzC3EFUke5CvYXVwn3CZwI+jVbBv4n3xE=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", groupByHelper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void buildSketchesAtIngestionTimeTimeseries() throws Exception
  {
    Sequence<Result> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Collections.singletonList("dim"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        buildAggregatorJson("HLLSketchBuild", "id", !ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQueryJson("HLLSketchMerge", "sketch", !ROUND)
    );
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "id", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketch;

    if (stringEncoding == StringEncoding.UTF16LE) {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAcoDCQOUmAsElgIEcSjfBAmeJQYLkjgKDFoJBQ6yWQUP9hIPONtCBxPgngsW9l4IGfQIChp8/wQbxkIHHXaYC+0y0QeE"
          + "PH8EJ3ATBSrq7wYrds8ELKKxBjH4RAszAMEFOJLSDzkQqgg6gNUEP0KEBwlswAVDtA0d6bbCB0U83gTMe7QKR4SSC0mAfQhKUuEFUHDLBd"
          + "50mwRZqCoUW26JC144/AVlpqEEcer/Bmd8iQlofI0HtGs5BWu4Xgdt6CgEbj5zBnG+3wd1WOMIeearB3v4dQh82FMGpq/NB39egwmAcM4I"
          + "voNMBYTIbwSLCoAIjYZiBo5gZAev4osP6VBHBoQQHAQ8tXoGZmZUB6H1EQWgLGEPoSzVD6XgLAmqVGkEqwzWB8zF3Qq0jDEKtiwdDrdSjB"
          + "W5CqMHvc4gDL467AfB4kgHA349BZZcHAvG5scKye6+B8sQZwfM8A8HzpIyBMsI8AiJSYcH1V4/BtiQxQbewoYLIr2VChaWmQfn8FwH6VgK"
          + "B+00gQZLS08G+Ji8C/lItQb6QmIK+2IzBf2+JQT+HtIOAY01DAIFAwUHp1wE9WGXBwnvuwsL55sLDd3KCg/dngUQwbIHEUukBR/Zuwci7c"
          + "kFJedPBSrf/hIraZUHLn/tBC9zAgUwKTQJMttbBDQ/7gQ2SRMKOC35DDxvtRNC0Q8T9U2hBESpTAvGBMkMS/2BB03hbgROwWgJT/M3BlOn"
          + "MQ9UTcQGVe88BFcdogRbf6gTXJnMBmEjsQRjnzcRZPO0BWVzCgZmLSIaIo8YBL9nowV7OYcEgVUdF4bHxQiJs1sFgRnMCJCjBQiayxUHnB"
          + "PNBqHF0AWkxZMMpaOPCKaz/gWpt3sFqhHKHa2xnAuvBeYEsRPiC7RFHwa4Ba0GvLcOB74/BQ2/L2MEwLN6BgR8uwZrstIKxXOgCso74gbM"
          + "ZRoLz9cyBNEPnwfVg28G1jWXBRp4PwXJHJoL7dzxB93jghPp7r0M459kB74n6Abn56sOOGLTDOphoAf+2FUF8hm6BvSHpQr1M0UH4BX8BP"
          + "kTeQX602UF/ZESEf/DRQs=";
    } else {
      // UTF-8
      expectedSketch =
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDgsQ4gcOMh8IEJhlExuaBgd+R1EHL/iCBLnt1AYyAMoGNIzUBDWsAwaLVYkGN4iHBzusLAY"
          + "/NKsGROa/Bkbi9gxHzIwHSXg2B6qmDwZRZDYEVZ63CVZobAtY2FMEWkBtBl6WJghogCcFadYrBnfeJAh4GB4RfVy/G4B4UAqF4KwYhyJr"
          + "DjyZYg2LZEEWjWLSBY8s2Q6QEOALkoD0CpMWJAi5t5kF2tzKBJfExgaZaP8EnmRcBqMORAXjgo8NpnaCBKmqrAWq0tcPrtYqD6+8sAmyb"
          + "MoHufnhBbiMOQW53G4Fwk4JFcQy2QnHhJ4Uywa4DtEkEQbU9qIL2STtD9rGVwTbYAUI2wIoBd1GHAbi8uYF48BYEuZUJAyJvKgG6ZwIBu"
          + "sYkRTskNwFAO6cFbk9MQX4wqoa6bSaBQD2/Qr8riYH/lKSDP9OqAkA+5cHAR/DBgIB7hgIc04JDoWnEg/H5QoRlZkVEm2xBhS/lQYa/b8"
          + "THXcwBh9TsgUikxwQ/khMBCWdRR6InzcKJ8vCC35VGwUqaccLKx07BC19CQYuj2IJMS9dBjJfJw4qC5oFOfWCCYtcQQw88agZPvGICkED"
          + "HQxN6WwOThknDnivTwkQgPIEUitrCFcFXgRbrZEOZOOvBWcHnAxpz/YGa1UbBm2zqRFx0bAedi8RBncT8g54FasN/jwYBHtNgwR95xAEf"
          + "r1KGn9fCwaFaRsKhvktCIj36guL8yAbUXQ6BY/vvAaRnycFkw15G5SxVQaXU+YFmAdSEZojUw+cyxUQngNlD8C98wox3XQG3o/LBKs1NA"
          + "us+WAOr6UXCrL3vBc5eRwJtc1fCrerRQhSZ+oTuWlaCLzdnAu9a34Gv0GgDsB/VQbs1pQEwzn3BsW/iASZWK4FyJGOBslJdAzL0ZsQ49I"
          + "FBM13ZATPOZERL6IvEtPxkQXUrego1rfqBNgL/QkOmKME3WHsCd4NEB7fl2IP4Q2PCeKjfQfkpWoU6McWBumlohLrF0cG8eOEBPMLcQVS"
          + "R7kK9hdXCVJJ9Qf6NVsG9wmcCP4n3xE="
      ;
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", groupByHelper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void buildSketchesAtQueryTimeTimeseries() throws Exception
  {
    Sequence<Result> seq = timeseriesHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildTimeseriesQueryJson("HLLSketchBuild", "id", !ROUND)
    );
    List<Result> results = seq.toList();
    Assert.assertEquals(1, results.size());
    Result row = results.get(0);
    Assert.assertEquals(200, (double) ((TimeseriesResultValue) row.getValue()).getMetric("sketch"), 0.1);
  }

  @Test
  public void unsuccessfulComplexTypesInHLL() throws Exception
  {
    String metricSpec = "[{"
                        + "\"type\": \"hyperUnique\","
                        + "\"name\": \"index_hll\","
                        + "\"fieldName\": \"id\""
                        + "}]";
    try {
      Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
          new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
          buildParserJson(
              Arrays.asList("dim", "multiDim", "id"),
              Arrays.asList("timestamp", "dim", "multiDim", "id")
          ),
          metricSpec,
          0, // minTimestamp
          Granularities.NONE,
          200, // maxRowCount
          buildGroupByQueryJson("HLLSketchMerge", "sketch", ROUND, stringEncoding)
      );
    }
    catch (RuntimeException e) {
      Assert.assertTrue(
          e.getMessage().contains("Invalid input [index_hll] of type [COMPLEX<hyperUnique>] for [HLLSketchBuild]"));
    }
  }

  @Test
  public void buildSketchesAtQueryTimeMultiValue() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "multiDim", !ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(14, (double) row.get(0), 0.1);

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketch;

    if (stringEncoding == StringEncoding.UTF16LE) {
      expectedSketch = "AwEHDAUIAAEOAAAAhDx/BKWjjwiJs1sFRTzeBMnuvgfYkMUGyRyaC39egwmJSYcHOGLTDDkQqgg6gNUEGfQICj9ChAc=";
    } else {
      expectedSketch = "AwEHDAUIAAEOAAAAwH9VBlJJ9QfJSXQMqqYPBq7WKg+PLNkOsve8F9PxkQXYC/0JOfWCCbzdnAsddzAG/lKSDB9TsgU=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", groupByHelper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void roundBuildSketch() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_raw.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim", "id"),
            Arrays.asList("timestamp", "dim", "multiDim", "id")
        ),
        "[]",
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchBuild", "id", ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200L, (long) row.get(0));

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketch;

    if (stringEncoding == StringEncoding.UTF16LE) {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAcoDCQOUmAsElgIEcSjfBAmeJQYLkjgKDFoJBQ6yWQUP9hIPONtCBxPgngsW9l4IGfQIChp8/wQbxkIHHXaYC+0y0QeEP"
          + "H8EJ3ATBSrq7wYrds8ELKKxBjH4RAszAMEFOJLSDzkQqgg6gNUEP0KEBwlswAVDtA0d6bbCB0U83gTMe7QKR4SSC0mAfQhKUuEFUHDLBd50"
          + "mwRZqCoUW26JC144/AVlpqEEcer/Bmd8iQlofI0HtGs5BWu4Xgdt6CgEbj5zBnG+3wd1WOMIeearB3v4dQh82FMGpq/NB39egwmAcM4IvoN"
          + "MBYTIbwSLCoAIjYZiBo5gZAev4osP6VBHBoQQHAQ8tXoGZmZUB6H1EQWgLGEPoSzVD6XgLAmqVGkEqwzWB8zF3Qq0jDEKtiwdDrdSjBW5Cq"
          + "MHvc4gDL467AfB4kgHA349BZZcHAvG5scKye6+B8sQZwfM8A8HzpIyBMsI8AiJSYcH1V4/BtiQxQbewoYLIr2VChaWmQfn8FwH6VgKB+00g"
          + "QZLS08G+Ji8C/lItQb6QmIK+2IzBf2+JQT+HtIOAY01DAIFAwUHp1wE9WGXBwnvuwsL55sLDd3KCg/dngUQwbIHEUukBR/Zuwci7ckFJedP"
          + "BSrf/hIraZUHLn/tBC9zAgUwKTQJMttbBDQ/7gQ2SRMKOC35DDxvtRNC0Q8T9U2hBESpTAvGBMkMS/2BB03hbgROwWgJT/M3BlOnMQ9UTcQ"
          + "GVe88BFcdogRbf6gTXJnMBmEjsQRjnzcRZPO0BWVzCgZmLSIaIo8YBL9nowV7OYcEgVUdF4bHxQiJs1sFgRnMCJCjBQiayxUHnBPNBqHF0A"
          + "WkxZMMpaOPCKaz/gWpt3sFqhHKHa2xnAuvBeYEsRPiC7RFHwa4Ba0GvLcOB74/BQ2/L2MEwLN6BgR8uwZrstIKxXOgCso74gbMZRoLz9cyB"
          + "NEPnwfVg28G1jWXBRp4PwXJHJoL7dzxB93jghPp7r0M459kB74n6Abn56sOOGLTDOphoAf+2FUF8hm6BvSHpQr1M0UH4BX8BPkTeQX602UF"
          + "/ZESEf/DRQs=";
    } else {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDotkQRYLEOIHDjIfCBCYZRMbmgYHfkdRB7lpWgj+UpIML/iCBLnt1AYyAMoGNIzUBDWsAwaLVYkGN4iHBzusLAY/NKsGROa/Bkbi9gxHzIwHSXg2B6qmDwZRdDoFVZ63CVZobAtY2FMEWkBtBl6WJghogCcFadYrBnfeJAh4GB4RfVy/G4B4UAqcyxUQheCsGIciaw6JvKgGi1xBDI1i0gWPLNkOkBDgC5KA9AqTFiQIl8TGBplo/wSeZFwGow5EBeOCjw2mdoIEqaqsBarS1w+u1ioPr7ywCbJsyge5+eEFuIw5BbncbgXaxlcEwk4JFcQy2QnHhJ4Uywa4DtEkEQbU9qIL2STtD9rcygTbYAUI2wIoBd1GHAbi8uYF48BYEuZUJAxSK2sI6bSaBesYkRTs1pQEAO6cFbk9MQX4wqoaAPb9CvyuJgf+SEwE/06oCQD7lwcBH8MGAgHuGAhzTgkOhacSD8flChGVmRUSbbEGFL+VBhr9vxMddzAGH1OyBSKTHBAlnUUeiJ83CifLwgt+VRsFKmnHCysdOwQtfQkGLo9iCTEvXQYyXycOKguaBeyQ3AXpnAgGOfWCCTyZYg0+8YgKQQMdDE3pbA5OGScOeK9PCRCA8gRSSfUHVwVeBFutkQ5k468FZwecDGnP9gZrVRsGbbOpEXHRsB52LxEGdxPyDngVqw3+PBgEe02DBH3nEAR+vUoaf18LBlFkNgSFaRsKhvktCIj36guL8yAbj++8BpGfJwWTDXkblLFVBpdT5gWYB1IRmiNTD7XNXwqeA2UPwL3zCjHddAbej8sEqzU0C6z5YA6vpRcKsve8Fzl5HAk88agZt6tFCFJn6hO5t5kFvN2cC71rfga/QaAOwH9VBsM59wbFv4gEmViuBciRjgbJSXQMy9GbEOPSBQTNd2QEzzmRES+iLxLT8ZEF1K3oKNa36gTYC/0JDpijBN1h7AneDRAe35diD+ENjwnio30H5KVqFOjHFgbppaIS6xdHBvHjhATzC3EFUke5CvYXVwn3CZwI+jVbBv4n3xE=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", groupByHelper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void roundMergeSketch() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        buildGroupByQueryJson("HLLSketchMerge", "sketch", ROUND, stringEncoding)
    );
    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200L, (long) row.get(0));

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketch =
        "AwEHDAkIAAHIAAAAAiK0BAWIUw8GQkoICxROBBVEtgcXLiAGGbKkCRq80AUbIqkIH3iCByBm4xIlXqYJK/L7BixGBQYu5AsOLxoTBzSiYQ42"
        + "KrIEN6znB7AS2wY/kEgE/JC2BUZKtwRH1rMNSizzBE744gVTYroEuD/5B9IWcwdeersMY+ISCNeMnQ/VIhcTakA7DWvM/gR0XmkKdwL+B+"
        + "AxWRB7iqkJfHS5B4G8XQbeUy0E8ranC4WKqwuHtqoEimJoFo3EiQmT7KIJlH6hFJQHwgSaiDcHnQpsD57kmxjOoO8FoXrkCdSLjASkeLMH"
        + "qE7tBKlYMwmqPmMHrjyIEbjsbwmwPp0Hsqi4BrVMrQ+4VqkMk8R2Bb0gUg6+9PgGv842FMOcmh/GYjQHyHokBMqMXgXNWtkMzvBbHzBDXw"
        + "7SfIwR1VKVCdd4NAbaXBIH21ItBNxaAQbd9tsG31Z4EXvL9gfppI4TZBO3BvJ8HAb5+PEE+a6eD/yaMREGfe8RDF8FBQ1fegkRy64GFMeI"
        + "DxcFewTDsDgEIKkZDiI76wUkz84NKjnxBi9TdhMwRVkLMiViBTWpMQQ2RwkH4K6sCDmLKAc7aa4EPtleBkD7DQ5B19cISe1qBoGehQfp0i"
        + "gFU4PaB1R7Rw9Zf9QN753zEPTT8gthmacHZD0WBWXfihDyeFwMbeUdB27FNAZvy0oHcMl2B3FDyAl1gWYHed9lBHtl5ggfuiIIfSM8CoDB"
        + "8wmBrzIEhGnMBYYv+Q2IJ6AEiV2yDI1JrAWPw7AG9D3fD5TDIQSXvygMmUlCBJq7YwSeLWoHn6ugDMPdUQSj85wKpG+rBqYRUQSpO7UHqn"
        + "OHFq23zAiwW0YS58+VF7YvCQa3Gc8HuAH8Erl/PgW7yQEFvhmBBQZzZQTB6RcFwnHPBakaJQfEtZ8HxbGmEcYZagTL18IEywPsBs5JcwXP"
        + "4UgE0U97BdMTZwXUOwoH1ZcUB9bP8QY7vb0M2RtKCNsXmgrdn3oL3tsZC+DtSQfhpXQG7eg9BeYLowvnUe4Ek1QxBe8t9wfzofoJl7tgGv"
        + "XpZg32cfIG+2nDBvwtQgo=";

    Assert.assertEquals("\"" + expectedSketch + "\"", groupByHelper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void testPostAggs() throws Exception
  {
    Sequence<ResultRow> seq = groupByHelper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        groupByHelper.getObjectMapper().writeValueAsString(
            GroupByQuery.builder()
                        .setDataSource("test_datasource")
                        .setGranularity(Granularities.ALL)
                        .setInterval(Intervals.ETERNITY)
                        .setAggregatorSpecs(
                            new HllSketchMergeAggregatorFactory("sketch", "sketch", null, null, null, null, false)
                        )
                        .setPostAggregatorSpecs(
                            ImmutableList.of(
                                new HllSketchToEstimatePostAggregator(
                                    "estimate",
                                    new FieldAccessPostAggregator("f1", "sketch"),
                                    false
                                ),
                                new HllSketchToEstimateWithBoundsPostAggregator(
                                    "estimateWithBounds",
                                    new FieldAccessPostAggregator(
                                        "f1",
                                        "sketch"
                                    ),
                                    2
                                ),
                                new HllSketchToStringPostAggregator(
                                    "summary",
                                    new FieldAccessPostAggregator("f1", "sketch")
                                ),
                                new HllSketchUnionPostAggregator(
                                    "union",
                                    ImmutableList.of(new FieldAccessPostAggregator(
                                        "f1",
                                        "sketch"
                                    ), new FieldAccessPostAggregator("f2", "sketch")),
                                    null,
                                    null
                                ),
                                new FieldAccessPostAggregator("fieldAccess", "sketch")
                            )
                        )
                        .build()
        )
    );
    final String expectedSummary = "### HLL SKETCH SUMMARY: \n"
                                   + "  Log Config K   : 12\n"
                                   + "  Hll Target     : HLL_4\n"
                                   + "  Current Mode   : SET\n"
                                   + "  Memory         : false\n"
                                   + "  LB             : 200.0\n"
                                   + "  Estimate       : 200.0000988444255\n"
                                   + "  UB             : 200.01008469948434\n"
                                   + "  OutOfOrder Flag: false\n"
                                   + "  Coupon Count   : 200\n";

    // Check specific sketch: result should be deterministic given same inputs in same order. This helps makes sure
    // that our logic remains compatible across versions.

    final String expectedSketchBase64 =
        "AwEHDAkIAAHIAAAAAiK0BAWIUw8GQkoICxROBBVEtgcXLiAGGbKkCRq80AUbIqkIH3iCByBm4xIlXqYJK/L7BixGBQYu5AsOLxoTBzSiYQ42"
        + "KrIEN6znB7AS2wY/kEgE/JC2BUZKtwRH1rMNSizzBE744gVTYroEuD/5B9IWcwdeersMY+ISCNeMnQ/VIhcTakA7DWvM/gR0XmkKdwL+B+"
        + "AxWRB7iqkJfHS5B4G8XQbeUy0E8ranC4WKqwuHtqoEimJoFo3EiQmT7KIJlH6hFJQHwgSaiDcHnQpsD57kmxjOoO8FoXrkCdSLjASkeLMH"
        + "qE7tBKlYMwmqPmMHrjyIEbjsbwmwPp0Hsqi4BrVMrQ+4VqkMk8R2Bb0gUg6+9PgGv842FMOcmh/GYjQHyHokBMqMXgXNWtkMzvBbH9J8jB"
        + "HVUpUJ13g0BtpcEgfbUi0E3FoBBt322wbfVngRe8v2B+mkjhNkE7cG8nwcBvn48QT5rp4P/JoxEQZ97xEMXwUFDV96CRHLrgYUx4gPFwV7"
        + "BMOwOAQgqRkOIjvrBSTPzg0qOfEG753zEC9TdhMwQ18OMiViBTWpMQQ2RwkH4K6sCDmLKAc7aa4EPtleBkD7DQ5B19cISe1qBoGehQfp0i"
        + "gFU4PaB1R7Rw9Zf9QN0xNnBfTT8gthmacHZD0WBWXfihDyeFwMbeUdB27FNAZvy0oHcMl2B3FDyAl1gWYHed9lBHtl5ggfuiIIfSM8CoDB"
        + "8wmBrzIEhGnMBYYv+Q2IJ6AEiV2yDI1JrAWPw7AG9D3fD5TDIQSXvygMmUlCBJq7YwSeLWoHn6ugDMPdUQSj85wKpG+rBqYRUQSpO7UHqn"
        + "OHFq23zAiwW0YS58+VF7YvCQa3Gc8HuAH8Erl/PgW7yQEFvhmBBQZzZQTB6RcFwnHPBakaJQfEtZ8HxbGmEcYZagTL18IEywPsBs5JcwXP"
        + "4UgE0U97BTBFWQvUOwoH1ZcUB9bP8QY7vb0M2RtKCNsXmgrdn3oL3tsZC+DtSQfhpXQG7eg9BeYLowvnUe4Ek1QxBe8t9wfzofoJl7tgGv"
        + "XpZg32cfIG+2nDBvwtQgo=";

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
    Assert.assertEquals(200, (double) row.get(1), 0.1);
    Assert.assertArrayEquals(new double[]{200, 200, 200}, (double[]) row.get(2), 0.1);
    Assert.assertEquals(expectedSummary, row.get(3));
    // union with self = self
    Assert.assertEquals(expectedSummary, ((HllSketchHolder) row.get(4)).getSketch().toString());
    Assert.assertEquals(
        "\"" + expectedSketchBase64 + "\"",
        groupByHelper.getObjectMapper().writeValueAsString(row.get(4))
    );
    Assert.assertEquals(
        "\"" + expectedSketchBase64 + "\"",
        groupByHelper.getObjectMapper().writeValueAsString(row.get(5))
    );
  }

  private static String buildParserJson(List<String> dimensions, List<String> columns)
  {
    Map<String, Object> timestampSpec = ImmutableMap.of(
        "column", "timestamp",
        "format", "yyyyMMdd"
    );
    Map<String, Object> dimensionsSpec = ImmutableMap.of(
        "dimensions", dimensions,
        "dimensionExclusions", Collections.emptyList(),
        "spatialDimensions", Collections.emptyList()
    );
    Map<String, Object> parseSpec = ImmutableMap.of(
        "format", "tsv",
        "timestampSpec", timestampSpec,
        "dimensionsSpec", dimensionsSpec,
        "columns", columns,
        "listDelimiter", ","
    );
    Map<String, Object> object = ImmutableMap.of(
        "type", "string",
        "parseSpec", parseSpec
    );
    return toJson(object);
  }

  private static String toJson(Object object)
  {
    final String json;
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(object);
    }
    catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    return json;
  }

  private static String buildAggregatorJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    Map<String, Object> aggregator = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        stringEncoding
    );
    return toJson(Collections.singletonList(aggregator));
  }

  private static Map<String, Object> buildAggregatorObject(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    return ImmutableMap.of(
        "type", aggregationType,
        "name", "sketch",
        "fieldName", aggregationFieldName,
        "round", aggregationRound,
        "stringEncoding", stringEncoding.toString()
    );
  }

  private String buildGroupByQueryJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound,
      StringEncoding stringEncoding
  )
  {
    Map<String, Object> aggregation = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        stringEncoding
    );
    Map<String, Object> object = new ImmutableMap.Builder<String, Object>()
        .put("queryType", "groupBy")
        .put("dataSource", "test_dataSource")
        .put("granularity", "ALL")
        .put("dimensions", Collections.emptyList())
        .put("aggregations", Collections.singletonList(aggregation))
        .put(
            "postAggregations",
            Collections.singletonList(
                ImmutableMap.of("type", "fieldAccess", "name", "sketch_raw", "fieldName", "sketch")
            )
        )
        .put("intervals", Collections.singletonList("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z"))
        .put("context", ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
        .build();
    return toJson(object);
  }

  private String buildTimeseriesQueryJson(
      String aggregationType,
      String aggregationFieldName,
      boolean aggregationRound
  )
  {
    Map<String, Object> aggregation = buildAggregatorObject(
        aggregationType,
        aggregationFieldName,
        aggregationRound,
        HllSketchAggregatorFactory.DEFAULT_STRING_ENCODING
    );
    Map<String, Object> object = new ImmutableMap.Builder<String, Object>()
        .put("queryType", "timeseries")
        .put("dataSource", "test_dataSource")
        .put("granularity", "ALL")
        .put("aggregations", Collections.singletonList(aggregation))
        .put("intervals", Collections.singletonList("2017-01-01T00:00:00.000Z/2017-01-31T00:00:00.000Z"))
        .put("context", ImmutableMap.of(QueryContexts.VECTORIZE_KEY, vectorize.toString()))
        .build();
    return toJson(object);
  }

}
