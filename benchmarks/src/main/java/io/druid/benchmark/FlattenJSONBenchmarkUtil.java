package io.druid.benchmark;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.wnameless.json.flattener.JsonFlattener;

import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.JSONPathFieldSpec;
import io.druid.data.input.impl.JSONPathSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.parsers.Parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FlattenJSONBenchmarkUtil
{
  private Random rng;
  private final ObjectMapper mapper = new DefaultObjectMapper();
  private static final String DEFAULT_TIMESTAMP = "2015-09-12T12:10:53.155Z";

  public FlattenJSONBenchmarkUtil()
  {
    this.rng = new Random(9999);
    mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.PUBLIC_ONLY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
  }

  public Parser getFlatParser()
  {
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(null, null, null),
        null,
        null
    );
    return spec.makeParser();
  }

  public Parser getFieldDiscoveryParser()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    JSONPathSpec flattenSpec = new JSONPathSpec(true, fields);

    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(null, null, null),
        flattenSpec,
        null
    );

    return spec.makeParser();
  }

  public Parser getNestedParser()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(JSONPathFieldSpec.createRootField("ts"));

    fields.add(JSONPathFieldSpec.createRootField("d1"));
    //fields.add(JSONPathFieldSpec.createRootField("d2"));
    fields.add(JSONPathFieldSpec.createNestedField("e1.d1", "$.e1.d1"));
    fields.add(JSONPathFieldSpec.createNestedField("e1.d2", "$.e1.d2"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d3", "$.e2.d3"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d4", "$.e2.d4"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d5", "$.e2.d5"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d6", "$.e2.d6"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[0]", "$.e2.ad1[0]"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[1]", "$.e2.ad1[1]"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[2]", "$.e2.ad1[2]"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[0].d1", "$.ae1[0].d1"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[1].d1", "$.ae1[1].d1"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[2].e1.d2", "$.ae1[2].e1.d2"));

    fields.add(JSONPathFieldSpec.createRootField("m3"));
    //fields.add(JSONPathFieldSpec.createRootField("m4"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m1", "$.e3.m1"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m2", "$.e3.m2"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m3", "$.e3.m3"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m4", "$.e3.m4"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[0]", "$.e3.am1[0]"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[1]", "$.e3.am1[1]"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[2]", "$.e3.am1[2]"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[3]", "$.e3.am1[3]"));
    fields.add(JSONPathFieldSpec.createNestedField("e4.e4.m4", "$.e4.e4.m4"));

    JSONPathSpec flattenSpec = new JSONPathSpec(true, fields);
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(null, null, null),
        flattenSpec,
        null
    );

    return spec.makeParser();
  }

  public Parser getForcedPathParser()
  {
    List<JSONPathFieldSpec> fields = new ArrayList<>();
    fields.add(JSONPathFieldSpec.createNestedField("ts", "$['ts']"));

    fields.add(JSONPathFieldSpec.createNestedField("d1", "$['d1']"));
    fields.add(JSONPathFieldSpec.createNestedField("d2", "$['d2']"));
    fields.add(JSONPathFieldSpec.createNestedField("e1.d1", "$['e1.d1']"));
    fields.add(JSONPathFieldSpec.createNestedField("e1.d2", "$['e1.d2']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d3", "$['e2.d3']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d4", "$['e2.d4']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d5", "$['e2.d5']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.d6", "$['e2.d6']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[0]", "$['e2.ad1[0]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[1]", "$['e2.ad1[1]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e2.ad1[2]", "$['e2.ad1[2]']"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[0].d1", "$['ae1[0].d1']"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[1].d1", "$['ae1[1].d1']"));
    fields.add(JSONPathFieldSpec.createNestedField("ae1[2].e1.d2", "$['ae1[2].e1.d2']"));

    fields.add(JSONPathFieldSpec.createNestedField("m3", "$['m3']"));
    fields.add(JSONPathFieldSpec.createNestedField("m4", "$['m4']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m1", "$['e3.m1']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m2", "$['e3.m2']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m3", "$['e3.m3']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.m4", "$['e3.m4']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[0]", "$['e3.am1[0]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[1]", "$['e3.am1[1]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[2]", "$['e3.am1[2]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e3.am1[3]", "$['e3.am1[3]']"));
    fields.add(JSONPathFieldSpec.createNestedField("e4.e4.m4", "$['e4.e4.m4']"));

    JSONPathSpec flattenSpec = new JSONPathSpec(false, fields);
    JSONParseSpec spec = new JSONParseSpec(
        new TimestampSpec("ts", "iso", null),
        new DimensionsSpec(null, null, null),
        flattenSpec,
        null
    );

    return spec.makeParser();
  }


  public String generateFlatEvent() throws Exception
  {
    String nestedEvent = generateNestedEvent();
    String flatEvent = JsonFlattener.flatten(nestedEvent);
    return flatEvent;
  }

  /*
  e.g.,

  {
  "d1":"-889954295",
  "d2":"-1724267856",
  "m3":0.1429096312550323,
  "m4":-7491190942271782800,
  "e1":{"d1":"2044704643",
        "d2":"743384585"},
  "e2":{"d3":"1879234327",
        "d4":"1248394579",
        "d5":"-639742676",
        "d6":"1334864967",
        "ad1":["-684042233","-1368392605","1826364033"]},
  "e3":{"m1":1026394465228315487,
        "m2":0.27737174619459004,
        "m3":0.011921350960908628,
        "m4":-7507319256575520484,
        "am1":[-2383262648875933574,-3980663171371801209,-8225906222712163481,6074309311406287835]},
  "e4":{"e4":{"m4":32836881083689842}},
  "ae1":[{"d1":"-1797792200"},{"d1":"142582995"},{"e1":{"d2":"-1341994709"}}],
  "ts":"2015-09-12T12:10:53.155Z"
  }
  */
  public String generateNestedEvent() throws Exception
  {
    BenchmarkEvent nestedDims1 = new BenchmarkEvent(
        null,
        String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()), null, null, null, null,
        null, null, null, null,
        null, null, null, null,
        null, null, null
    );

    String[] dimsArray1 = {String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt())};
    BenchmarkEvent nestedDims2 = new BenchmarkEvent(
        null,
        null, null, String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()),
        null, null, null, null,
        null, null, null, null,
        dimsArray1, null, null
    );

    Long[] metricsArray1 = {rng.nextLong(), rng.nextLong(), rng.nextLong(), rng.nextLong()};
    BenchmarkEvent nestedMetrics1 = new BenchmarkEvent(
        null,
        null, null, null, null, null, null,
        rng.nextLong(), rng.nextDouble(), rng.nextDouble(), rng.nextLong(),
        null, null, null, null,
        null, metricsArray1, null
    );

    BenchmarkEvent nestedMetrics2 = new BenchmarkEvent(
        null,
        null, null, null, null, null, null,
        null, null, null, rng.nextLong(),
        null, null, null, null,
        null, null, null
    );

    BenchmarkEvent metricsWrapper = new BenchmarkEvent(
        null,
        null, null, null, null, null, null,
        null, null, null, null,
        null, null, null, nestedMetrics2,
        null, null, null
    );

    //nest some dimensions in an array!
    BenchmarkEvent arrayNestedDim1 = new BenchmarkEvent(
        null,
        String.valueOf(rng.nextInt()), null, null, null, null, null,
        null, null, null, null,
        null, null, null, null,
        null, null, null
    );
    BenchmarkEvent arrayNestedDim2 = new BenchmarkEvent(
        null,
        String.valueOf(rng.nextInt()), null, null, null, null, null,
        null, null, null, null,
        null, null, null, null,
        null, null, null
    );
    BenchmarkEvent arrayNestedDim3 = new BenchmarkEvent(
        null,
        null, String.valueOf(rng.nextInt()), null, null, null, null,
        null, null, null, null,
        null, null, null, null,
        null, null, null
    );
    BenchmarkEvent arrayNestedWrapper = new BenchmarkEvent(
        null,
        null, null, null, null, null, null,
        null, null, null, null,
        arrayNestedDim3, null, null, null,
        null, null, null
    );
    BenchmarkEvent[] eventArray = {arrayNestedDim1, arrayNestedDim2, arrayNestedWrapper};

    Long[] ignoredMetrics = {Long.valueOf(10), Long.valueOf(20), Long.valueOf(30)};

    BenchmarkEvent wrapper = new BenchmarkEvent(
        DEFAULT_TIMESTAMP,
        String.valueOf(rng.nextInt()), String.valueOf(rng.nextInt()), null, null, null, null,
        null, null, rng.nextDouble(), rng.nextLong(),
        nestedDims1, nestedDims2, nestedMetrics1, metricsWrapper,
        null, ignoredMetrics, eventArray
    );

    return mapper.writeValueAsString(wrapper);
  }

  public class BenchmarkEvent
  {

    public String ts;

    @JsonProperty
    public String getTs()
    {
      return ts;
    }

    @JsonProperty
    public String getD1()
    {
      return d1;
    }

    @JsonProperty
    public String getD2()
    {
      return d2;
    }

    @JsonProperty
    public String getD3()
    {
      return d3;
    }

    @JsonProperty
    public String getD4()
    {
      return d4;
    }

    @JsonProperty
    public String getD5()
    {
      return d5;
    }

    @JsonProperty
    public String getD6()
    {
      return d6;
    }

    @JsonProperty
    public Long getM1()
    {
      return m1;
    }

    @JsonProperty
    public Double getM2()
    {
      return m2;
    }

    @JsonProperty
    public Double getM3()
    {
      return m3;
    }

    @JsonProperty
    public Long getM4()
    {
      return m4;
    }

    @JsonProperty
    public BenchmarkEvent getE1()
    {
      return e1;
    }

    @JsonProperty
    public BenchmarkEvent getE2()
    {
      return e2;
    }

    @JsonProperty
    public BenchmarkEvent getE3()
    {
      return e3;
    }

    @JsonProperty
    public BenchmarkEvent getE4()
    {
      return e4;
    }

    @JsonProperty
    public String[] getAd1()
    {
      return ad1;
    }

    @JsonProperty
    public Long[] getAm1()
    {
      return am1;
    }

    @JsonProperty
    public BenchmarkEvent[] getAe1()
    {
      return ae1;
    }

    public String d1;
    public String d2;
    public String d3;
    public String d4;
    public String d5;
    public String d6;
    public Long m1;
    public Double m2;
    public Double m3;
    public Long m4;
    public BenchmarkEvent e1;
    public BenchmarkEvent e2;
    public BenchmarkEvent e3;
    public BenchmarkEvent e4;
    public String[] ad1;
    public Long[] am1;
    public BenchmarkEvent[] ae1;

    @JsonCreator
    public BenchmarkEvent(
        @JsonProperty("ts") String ts,
        @JsonProperty("d1") String d1,
        @JsonProperty("d2") String d2,
        @JsonProperty("d3") String d3,
        @JsonProperty("d4") String d4,
        @JsonProperty("d5") String d5,
        @JsonProperty("d6") String d6,
        @JsonProperty("m1") Long m1,
        @JsonProperty("m2") Double m2,
        @JsonProperty("m3") Double m3,
        @JsonProperty("m4") Long m4,
        @JsonProperty("e1") BenchmarkEvent e1,
        @JsonProperty("e2") BenchmarkEvent e2,
        @JsonProperty("e3") BenchmarkEvent e3,
        @JsonProperty("e4") BenchmarkEvent e4,
        @JsonProperty("ad1") String[] ad1,
        @JsonProperty("am1") Long[] am1,
        @JsonProperty("ae1") BenchmarkEvent[] ae1
    )
    {
      this.ts = ts;
      this.d1 = d1;
      this.d2 = d2;
      this.d3 = d3;
      this.d4 = d4;
      this.d5 = d5;
      this.d6 = d6;
      this.m1 = m1;
      this.m2 = m2;
      this.m3 = m3;
      this.m4 = m4;
      this.e1 = e1;
      this.e2 = e2;
      this.e3 = e3;
      this.e4 = e4;
      this.ad1 = ad1;
      this.am1 = am1;
      this.ae1 = ae1;
    }
  }
}
