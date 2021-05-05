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
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.aggregation.AggregationTestHelper;
import org.apache.druid.query.aggregation.post.FieldAccessPostAggregator;
import org.apache.druid.query.groupby.GroupByQuery;
import org.apache.druid.query.groupby.GroupByQueryConfig;
import org.apache.druid.query.groupby.GroupByQueryRunnerTest;
import org.apache.druid.query.groupby.ResultRow;
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

  private final AggregationTestHelper helper;
  private final QueryContexts.Vectorize vectorize;
  private final StringEncoding stringEncoding;

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  public HllSketchAggregatorTest(GroupByQueryConfig config, String vectorize, StringEncoding stringEncoding)
  {
    HllSketchModule.registerSerde();
    helper = AggregationTestHelper.createGroupByQueryAggregationTestHelper(
        new HllSketchModule().getJacksonModules(), config, tempFolder);
    this.vectorize = QueryContexts.Vectorize.fromString(vectorize);
    this.stringEncoding = stringEncoding;
  }

  @Parameterized.Parameters(name = "groupByConfig = {0}, vectorize = {1}, stringEncoding = {2}")
  public static Collection<?> constructorFeeder()
  {
    final List<Object[]> constructors = new ArrayList<>();
    for (GroupByQueryConfig config : GroupByQueryRunnerTest.testConfigs()) {
      for (String vectorize : new String[]{"false", "true", "force"}) {
        for (StringEncoding stringEncoding : StringEncoding.values()) {
          constructors.add(new Object[]{config, vectorize, stringEncoding});
        }
      }
    }
    return constructors;
  }

  @Test
  public void ingestSketches() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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
  public void buildSketchesAtIngestionTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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
          "AwEHDAkIAAHIAAAAAcoDCQOUmAsElgIEcSjfBAlswAULkjgKDFoJBQ6yWQUP9hIPONtCBxPgngsWlpkHGfQIChp8/wQbxkIHHXaYC+0y0QeE"
          + "PH8EJ3ATBSrq7wYrds8ELKKxBjH4RAszAMEFOJLSDzkQqgg6gNUEP0KEB76DTAVDtA0d6bbCB0U83gTMe7QKR4SSC0mAfQhKUuEFUHDLBd"
          + "50mwRZqCoUW26JC/pCYgpeOPwFZaahBHHq/wZnfIkJaHyNB2uy0gpt6CgEbj5zBnG+3wd1WOMIeearB3v4dQh82FMGpq/NB39egwmAcM4I"
          + "hMhvBIsKgAiNhmIGjmBkB6/iiw/pUEcGhBAcBDy1egZmZlQHofURBaAsYQ+hLNUPpeAsCapUaQSrDNYHzMXdCrSMMQq2LB0Ot1KMFbkKow"
          + "e9ziAMvjrsB8HiSAcDfj0FllwcC8bmxwrJ7r4HyxBnB8zwDwfOkjIEywjwCIlJhwfVXj8G2JDFBt7ChgsivZUK5/BcB+lYCgftNIEGS0tP"
          + "BviYvAv5SLUGtEUfBvtiMwX9viUE/h7SDgGNNQwCBQMFB6dcBPVhlwcJ77sLC+ebCw3dygoP3Z4FEMGyBxFLpAUf2bsHIu3JBSXnTwX1M0"
          + "UHKt/+EitplQcuf+0EL3MCBTApNAky21sEND/uBDZJEwo4LfkMPG+1E0LRDxNEqUwLxgTJDEv9gQdN4W4ETsFoCU/zNwZTpzEPVE3EBlXv"
          + "PARXHaIEW3+oE1yZzAZhI7EEY583EWTztAVlcwoGZi0iGiKPGAS/Z6MFezmHBIFVHReGx8UIibNbBYEZzAiQowUIFvZeCJrLFQecE80Goc"
          + "XQBaTFkwylo48IprP+BQmeJQapt3sFqhHKHa2xnAuvBeYEsRPiC7RrOQW4Ba0GvLcOB74/BQ2/L2MEwLN6BgR8uwbFc6AKa7heB8o74gbM"
          + "ZRoLz9cyBNEPnwfVg28G1jWXBRp4PwXJHJoL7dzxB93jghPp7r0M459kB74n6Abn56sOOGLTDOphoAf+2FUF8hm6BvSHpQr1TaEE4BX8BP"
          + "kTeQX602UF/ZESEf/DRQs=";
    } else {
      expectedSketch =
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDotkQRYLEOIHDjIfCBCYZRMbmgYHfkdRB7lpWgj+UpIML/iCBLnt1AYyAMoGNIzUBDWsAwaL"
          + "VYkGN4iHBzusLAY/NKsGROa/Bkbi9gxHzIwHSXg2B6qmDwZRZDYEVZ63CVZobAtY2FMEWkBtBl6WJghogCcFadYrBnfeJAh4GB4RfVy/G8"
          + "B/VQaAeFAKheCsGIciaw6JvKgGi1xBDNa36gSNYtIFjyzZDpAQ4AuSgPQKkxYkCJfExgaZaP8EnmRcBqMORAXjgo8NpnaCBKmqrAWq0tcP"
          + "rtYqD6+8sAmybMoHufnhBbiMOQW53G4F2sZXBMJOCRXEMtkJx4SeFMsGuA7RJBEG1PaiC9kk7Q/a3MoE22AFCNsCKAXdRhwG4vLmBePAWB"
          + "LmVCQMUitrCOmcCAbrGJEU7NaUBADunBW5PTEF+MKqGgD2/Qr8riYH/khMBP9OqAkA+5cHAR/DBgIB7hgIc04JDoWnEg/H5QoRlZkVEm2x"
          + "BhS/lQYa/b8THXcwBh9TsgUikxwQJZ1FHoifNwony8ILflUbBSppxwsrHTsELX0JBi6PYgkxL10GMl8nDioLmgXskNwFOfWCCTzxqBk+8Y"
          + "gKQQMdDE3pbA5OGScOeK9PCRCA8gRSSfUHVwVeBFutkQ5k468FZwecDGnP9gZrVRsGbbOpEXHRsB52LxEGdxPyDngVqw3+PBgEe02DBH3n"
          + "EAR+vUoaf18LBoVpGwqG+S0IiPfqC4vzIBtRdDoFj++8BpGfJwWTDXkblLFVBpdT5gWYB1IRmiNTD5zLFRCeA2UP6bSaBTHddAbej8sEqz"
          + "U0C6z5YA6vpRcKsve8Fzl5HAm1zV8Kt6tFCFJn6hO5t5kFvN2cC71rfga/QaAOwL3zCsM59wbFv4gEmViuBciRjgbJSXQMy9GbEOPSBQTN"
          + "d2QEzzmRES+iLxLT8ZEF1K3oKDyZYg3YC/0JDpijBN1h7AneDRAe35diD+ENjwnio30H5KVqFOjHFgbppaIS6xdHBvHjhATzC3EFUke5Cv"
          + "YXVwn3CZwI+jVbBv4n3xE=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", helper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void buildSketchesAtQueryTime() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDotkQRYLEOIHDjIfCBCYZRMbmgYHfkdRB7lpWgj+UpIML/iCBLnt1AYyAMoGNIzUBDWsAwaL"
          + "VYkGN4iHBzusLAY/NKsGROa/Bkbi9gxHzIwHSXg2B6qmDwZRdDoFVZ63CVZobAtY2FMEWkBtBl6WJghogCcFadYrBnfeJAh4GB4RfVy/G4"
          + "B4UAqcyxUQheCsGIciaw6JvKgGi1xBDI1i0gWPLNkOkBDgC5KA9AqTFiQIl8TGBplo/wSeZFwGow5EBeOCjw2mdoIEqaqsBarS1w+u1ioP"
          + "r7ywCbJsyge5+eEFuIw5BbncbgXaxlcEwk4JFcQy2QnHhJ4Uywa4DtEkEQbU9qIL2STtD9rcygTbYAUI2wIoBd1GHAbi8uYF48BYEuZUJA"
          + "xSK2sI6bSaBesYkRTs1pQEAO6cFbk9MQX4wqoaAPb9CvyuJgf+SEwE/06oCQD7lwcBH8MGAgHuGAhzTgkOhacSD8flChGVmRUSbbEGFL+V"
          + "Bhr9vxMddzAGH1OyBSKTHBAlnUUeiJ83CifLwgt+VRsFKmnHCysdOwQtfQkGLo9iCTEvXQYyXycOKguaBeyQ3AXpnAgGOfWCCTyZYg0+8Y"
          + "gKQQMdDE3pbA5OGScOeK9PCRCA8gRSSfUHVwVeBFutkQ5k468FZwecDGnP9gZrVRsGbbOpEXHRsB52LxEGdxPyDngVqw3+PBgEe02DBH3n"
          + "EAR+vUoaf18LBlFkNgSFaRsKhvktCIj36guL8yAbj++8BpGfJwWTDXkblLFVBpdT5gWYB1IRmiNTD7XNXwqeA2UPwL3zCjHddAbej8sEqz"
          + "U0C6z5YA6vpRcKsve8Fzl5HAk88agZt6tFCFJn6hO5t5kFvN2cC71rfga/QaAOwH9VBsM59wbFv4gEmViuBciRjgbJSXQMy9GbEOPSBQTN"
          + "d2QEzzmRES+iLxLT8ZEF1K3oKNa36gTYC/0JDpijBN1h7AneDRAe35diD+ENjwnio30H5KVqFOjHFgbppaIS6xdHBvHjhATzC3EFUke5Cv"
          + "YXVwn3CZwI+jVbBv4n3xE=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", helper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void buildSketchesAtQueryTimeMultiValue() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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
      expectedSketch = "AwEHDAUIAAEOAAAAwH9VBslJdAyqpg8GrtYqD48s2Q6y97wX0/GRBR13MAbYC/0JOfWCCbzdnAtSSfUH/lKSDB9TsgU=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", helper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void roundBuildSketch() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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
          "AwEHDAkIAAHIAAAAAPp4CQGkVQwCbBkJBQKkDotkQRYLEOIHDjIfCBCYZRMbmgYHfkdRB7lpWgj+UpIML/iCBLnt1AYyAMoGNIzUBDWsAwaL"
          + "VYkGN4iHBzusLAY/NKsGROa/Bkbi9gxHzIwHSXg2B6qmDwZRdDoFVZ63CVZobAtY2FMEWkBtBl6WJghogCcFadYrBnfeJAh4GB4RfVy/G4"
          + "B4UAqcyxUQheCsGIciaw6JvKgGi1xBDI1i0gWPLNkOkBDgC5KA9AqTFiQIl8TGBplo/wSeZFwGow5EBeOCjw2mdoIEqaqsBarS1w+u1ioP"
          + "r7ywCbJsyge5+eEFuIw5BbncbgXaxlcEwk4JFcQy2QnHhJ4Uywa4DtEkEQbU9qIL2STtD9rcygTbYAUI2wIoBd1GHAbi8uYF48BYEuZUJA"
          + "xSK2sI6bSaBesYkRTs1pQEAO6cFbk9MQX4wqoaAPb9CvyuJgf+SEwE/06oCQD7lwcBH8MGAgHuGAhzTgkOhacSD8flChGVmRUSbbEGFL+V"
          + "Bhr9vxMddzAGH1OyBSKTHBAlnUUeiJ83CifLwgt+VRsFKmnHCysdOwQtfQkGLo9iCTEvXQYyXycOKguaBeyQ3AXpnAgGOfWCCTyZYg0+8Y"
          + "gKQQMdDE3pbA5OGScOeK9PCRCA8gRSSfUHVwVeBFutkQ5k468FZwecDGnP9gZrVRsGbbOpEXHRsB52LxEGdxPyDngVqw3+PBgEe02DBH3n"
          + "EAR+vUoaf18LBlFkNgSFaRsKhvktCIj36guL8yAbj++8BpGfJwWTDXkblLFVBpdT5gWYB1IRmiNTD7XNXwqeA2UPwL3zCjHddAbej8sEqz"
          + "U0C6z5YA6vpRcKsve8Fzl5HAk88agZt6tFCFJn6hO5t5kFvN2cC71rfga/QaAOwH9VBsM59wbFv4gEmViuBciRjgbJSXQMy9GbEOPSBQTN"
          + "d2QEzzmRES+iLxLT8ZEF1K3oKNa36gTYC/0JDpijBN1h7AneDRAe35diD+ENjwnio30H5KVqFOjHFgbppaIS6xdHBvHjhATzC3EFUke5Cv"
          + "YXVwn3CZwI+jVbBv4n3xE=";
    }

    Assert.assertEquals("\"" + expectedSketch + "\"", helper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void roundMergeSketch() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
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

    Assert.assertEquals("\"" + expectedSketch + "\"", helper.getObjectMapper().writeValueAsString(row.get(1)));
  }

  @Test
  public void testPostAggs() throws Exception
  {
    Sequence<ResultRow> seq = helper.createIndexAndRunQueryOnSegment(
        new File(this.getClass().getClassLoader().getResource("hll/hll_sketches.tsv").getFile()),
        buildParserJson(
            Arrays.asList("dim", "multiDim"),
            Arrays.asList("timestamp", "dim", "multiDim", "sketch")
        ),
        buildAggregatorJson("HLLSketchMerge", "sketch", ROUND, stringEncoding),
        0, // minTimestamp
        Granularities.NONE,
        200, // maxRowCount
        helper.getObjectMapper().writeValueAsString(
            GroupByQuery.builder()
                        .setDataSource("test_datasource")
                        .setGranularity(Granularities.ALL)
                        .setInterval(Intervals.ETERNITY)
                        .setAggregatorSpecs(
                            new HllSketchMergeAggregatorFactory("sketch", "sketch", null, null, null, false)
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
        + "AxWRB7iqkJfHS5B4G8XQbeUy0E8ranC4WKqwuHtqoEimJoFo3EiQmT7KIJlH6hFJQHwgSaiDcHnQpsD57kmxjOoO8FoXrkCdSLjASkeLMHq"
        + "E7tBKlYMwmqPmMHrjyIEbjsbwmwPp0Hsqi4BrVMrQ+4VqkMk8R2Bb0gUg6+9PgGv842FMOcmh/GYjQHyHokBMqMXgXNWtkMzvBbHzBDXw7S"
        + "fIwR1VKVCdd4NAbaXBIH21ItBNxaAQbd9tsG31Z4EXvL9gfppI4TZBO3BvJ8HAb5+PEE+a6eD/yaMREGfe8RDF8FBQ1fegkRy64GFMeIDxc"
        + "FewTDsDgEIKkZDiI76wUkz84NKjnxBi9TdhMwRVkLMiViBTWpMQQ2RwkH4K6sCDmLKAc7aa4EPtleBkD7DQ5B19cISe1qBoGehQfp0igFU4"
        + "PaB1R7Rw9Zf9QN753zEPTT8gthmacHZD0WBWXfihDyeFwMbeUdB27FNAZvy0oHcMl2B3FDyAl1gWYHed9lBHtl5ggfuiIIfSM8CoDB8wmBr"
        + "zIEhGnMBYYv+Q2IJ6AEiV2yDI1JrAWPw7AG9D3fD5TDIQSXvygMmUlCBJq7YwSeLWoHn6ugDMPdUQSj85wKpG+rBqYRUQSpO7UHqnOHFq23"
        + "zAiwW0YS58+VF7YvCQa3Gc8HuAH8Erl/PgW7yQEFvhmBBQZzZQTB6RcFwnHPBakaJQfEtZ8HxbGmEcYZagTL18IEywPsBs5JcwXP4UgE0U9"
        + "7BdMTZwXUOwoH1ZcUB9bP8QY7vb0M2RtKCNsXmgrdn3oL3tsZC+DtSQfhpXQG7eg9BeYLowvnUe4Ek1QxBe8t9wfzofoJl7tgGvXpZg32cf"
        + "IG+2nDBvwtQgo=";

    List<ResultRow> results = seq.toList();
    Assert.assertEquals(1, results.size());
    ResultRow row = results.get(0);
    Assert.assertEquals(200, (double) row.get(0), 0.1);
    Assert.assertEquals(200, (double) row.get(1), 0.1);
    Assert.assertArrayEquals(new double[]{200, 200, 200}, (double[]) row.get(2), 0.1);
    Assert.assertEquals(expectedSummary, row.get(3));
    // union with self = self
    Assert.assertEquals(expectedSummary, row.get(4).toString());
    Assert.assertEquals(
        "\"" + expectedSketchBase64 + "\"",
        helper.getObjectMapper().writeValueAsString(row.get(4))
    );
    Assert.assertEquals(
        "\"" + expectedSketchBase64 + "\"",
        helper.getObjectMapper().writeValueAsString(row.get(5))
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
}
