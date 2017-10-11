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

package io.druid.data.input.thrift;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.metamx.common.IAE;
import com.twitter.elephantbird.mapreduce.io.ThriftWritable;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.ParseSpec;
import org.apache.hadoop.io.BytesWritable;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.ByteBuffer;
import java.util.Map;

import io.druid.java.util.common.parsers.Parser;

/**
 * 1. load thrift class from classpath or provided jar
 * 2. deserialize content bytes and serialize to json
 * 3. use JsonSpec to do things left
 */
public class ThriftInputRowParser implements InputRowParser<Object>
{
  private final ParseSpec parseSpec;
  private final String jarPath;
  private final String thriftClassName;

  private Parser<String, Object> parser;
  volatile private Class<TBase> thriftClass = null;

  @JsonCreator
  public ThriftInputRowParser(
      @JsonProperty("parseSpec") ParseSpec parseSpec,
      @JsonProperty("thriftJar") String jarPath,
      @JsonProperty("thriftClass") String thriftClassName
  )
  {
    this.jarPath = jarPath;
    this.thriftClassName = thriftClassName;
    Preconditions.checkNotNull(thriftClassName, "thrift class name");

    this.parseSpec = parseSpec;
  }

  public Class<TBase> getThriftClass()
      throws IOException, ClassNotFoundException, IllegalAccessException, InstantiationException
  {
    final Class<TBase> thrift;
    if (jarPath != null) {
      File jar = new File(jarPath);
      URLClassLoader child = new URLClassLoader(
          new URL[]{jar.toURI().toURL()},
          this.getClass().getClassLoader()
      );
      thrift = (Class<TBase>) Class.forName(thriftClassName, true, child);
    } else {
      thrift = (Class<TBase>) Class.forName(thriftClassName);
    }
    thrift.newInstance();
    return thrift;
  }


  @Override
  public InputRow parse(Object input)
  {
    if (parser == null) {
      // parser should be created when it is really used to avoid unnecessary initialization of the underlying
      // parseSpec.
      parser = parseSpec.makeParser();
    }

    // There is a Parser check in phase 2 of mapreduce job, thrift jar may not present in peon side.
    // Place it this initialization in constructor will get ClassNotFoundException
    try {
      if (thriftClass == null) {
        thriftClass = getThriftClass();
      }
    }
    catch (IOException e) {
      throw new IAE(e, "failed to load jar [%s]", jarPath);
    }
    catch (ClassNotFoundException e) {
      throw new IAE(e, "class [%s] not found in jar", thriftClassName);
    }
    catch (InstantiationException | IllegalAccessException e) {
      throw new IAE(e, "instantiation thrift instance failed");
    }

    final String json;
    try {
      if (input instanceof ByteBuffer) { // realtime stream
        final byte[] bytes = ((ByteBuffer) input).array();
        TBase o = thriftClass.newInstance();
        ThriftDeserialization.detectAndDeserialize(bytes, o);
        json = ThriftDeserialization.SERIALIZER_SIMPLE_JSON.get().toString(o);
      } else if (input instanceof BytesWritable) { // sequence file
        final byte[] bytes = ((BytesWritable) input).getBytes();
        TBase o = thriftClass.newInstance();
        ThriftDeserialization.detectAndDeserialize(bytes, o);
        json = ThriftDeserialization.SERIALIZER_SIMPLE_JSON.get().toString(o);
      } else if (input instanceof ThriftWritable) { // LzoBlockThrift file
        TBase o = (TBase) ((ThriftWritable) input).get();
        json = ThriftDeserialization.SERIALIZER_SIMPLE_JSON.get().toString(o);
      } else {
        throw new IAE("unsupport input class of [%s]", input.getClass());
      }
    }
    catch (IllegalAccessException | InstantiationException | TException e) {
      throw new IAE("some thing wrong with your thrift?");
    }

    Map<String, Object> record = parser.parse(json);

    return new MapBasedInputRow(
        parseSpec.getTimestampSpec().extractTimestamp(record),
        parseSpec.getDimensionsSpec().getDimensionNames(),
        record
    );
  }

  @Override
  public ParseSpec getParseSpec()
  {
    return parseSpec;
  }

  @Override
  public InputRowParser withParseSpec(ParseSpec parseSpec)
  {
    return new ThriftInputRowParser(parseSpec, jarPath, thriftClassName);
  }
}
