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

package org.apache.druid.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.deser.DeserializationProblemHandler;
import com.fasterxml.jackson.databind.exc.InvalidTypeIdException;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.annotations.VisibleForTesting;
import org.apache.druid.java.util.common.StringUtils;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 */
public class DefaultObjectMapper extends ObjectMapper
{
  public static final DefaultObjectMapper INSTANCE = new DefaultObjectMapper();

  public DefaultObjectMapper()
  {
    this(null, null);
  }

  public DefaultObjectMapper(String serviceName)
  {
    this(null, serviceName);
  }

  public DefaultObjectMapper(DefaultObjectMapper mapper)
  {
    super(mapper);
  }

  public DefaultObjectMapper(JsonFactory factory, @Nullable String serviceName)
  {
    super(factory);
    registerModule(new DruidDefaultSerializersModule());
    registerModule(new GuavaModule());
    registerModule(new GranularityModule());
    registerModule(new AggregatorsModule());
    registerModule(new StringComparatorModule());
    registerModule(new SegmentizerModule());
    registerModule(new AppendableIndexModule());

    configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    configure(MapperFeature.AUTO_DETECT_GETTERS, false);
    // See https://github.com/FasterXML/jackson-databind/issues/170
    // configure(MapperFeature.AUTO_DETECT_CREATORS, false);
    configure(MapperFeature.AUTO_DETECT_FIELDS, false);
    configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
    configure(MapperFeature.AUTO_DETECT_SETTERS, false);
    configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false);
    configure(SerializationFeature.INDENT_OUTPUT, false);
    configure(SerializationFeature.FLUSH_AFTER_WRITE_VALUE, false);

    // Disable automatic JSON termination, so readers can detect truncated responses when a JsonGenerator is
    // closed after an exception is thrown while writing.
    configure(JsonGenerator.Feature.AUTO_CLOSE_JSON_CONTENT, false);

    addHandler(new DefaultDeserializationProblemHandler(serviceName));
  }

  @Override
  public ObjectMapper copy()
  {
    return new DefaultObjectMapper(this);
  }

  /**
   * A custom implementation of {@link #DeserializationProblemHandler} to add custom error message so
   * that users know how to troubleshoot unknown type ids.
   */
  static class DefaultDeserializationProblemHandler extends DeserializationProblemHandler
  {
    @Nullable
    private final String serviceName;

    public DefaultDeserializationProblemHandler(@Nullable String serviceName)
    {
      this.serviceName = serviceName;
    }

    @Nullable
    @VisibleForTesting
    String getServiceName()
    {
      return serviceName;
    }

    @Override
    public JavaType handleUnknownTypeId(DeserializationContext ctxt,
                                        JavaType baseType, String subTypeId, TypeIdResolver idResolver,
                                        String failureMsg)
        throws IOException
    {
      String serviceMsg = (serviceName == null) ? "" : StringUtils.format(" on '%s' service", serviceName);
      String msg = StringUtils.format("Please make sure to load all the necessary extensions and jars " +
              "with type '%s'%s. " +
              "Could not resolve type id '%s' as a subtype of %s",
          subTypeId, serviceMsg, subTypeId, ClassUtil.getTypeDescription(baseType));
      String extraFailureMsg = (failureMsg == null) ? msg : msg + " " + failureMsg;
      throw InvalidTypeIdException.from(ctxt.getParser(), extraFailureMsg, baseType, subTypeId);
    }
  }
}
