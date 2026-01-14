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

package org.apache.druid.guice;

import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.AnnotationIntrospectorPair;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.inject.Binder;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.MapBinder;
import org.apache.druid.frame.wire.FrameWireTransferable;
import org.apache.druid.guice.annotations.Deterministic;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.guice.annotations.JsonNonNull;
import org.apache.druid.guice.annotations.Smile;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.rowsandcols.RowsAndColumns;
import org.apache.druid.query.rowsandcols.concrete.FrameRowsAndColumns;
import org.apache.druid.query.rowsandcols.semantic.WireTransferable;
import org.apache.druid.query.rowsandcols.serde.RowsAndColumnsDeserializer;
import org.apache.druid.query.rowsandcols.serde.RowsAndColumnsSerializer;
import org.apache.druid.query.rowsandcols.serde.WireTransferableContext;

import javax.validation.Validator;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

@LazySingleton
public class DruidSecondaryModule implements Module
{
  public static final String PROPERTY_RAC_LEGACY_FRAME_SERIALIZATION = "druid.serde.rac.useLegacyFrameSerialization";

  public static MapBinder<ByteBuffer, WireTransferable.Deserializer> getWireTransferableDeserializerBinder(
      Binder binder
  )
  {
    return MapBinder.newMapBinder(
        binder,
        TypeLiteral.get(ByteBuffer.class),
        TypeLiteral.get(WireTransferable.Deserializer.class)
    );
  }

  private final Properties properties;
  private final ObjectMapper jsonMapper;
  private final ObjectMapper jsonMapperOnlyNonNullValueSerialization;
  private final ObjectMapper smileMapper;
  private final Validator validator;

  @Inject
  public DruidSecondaryModule(
      Properties properties,
      @Json ObjectMapper jsonMapper,
      @JsonNonNull ObjectMapper jsonMapperOnlyNonNullValueSerialization,
      @Smile ObjectMapper smileMapper,
      Validator validator
  )
  {
    this.properties = properties;
    this.jsonMapper = jsonMapper;
    this.jsonMapperOnlyNonNullValueSerialization = jsonMapperOnlyNonNullValueSerialization;
    this.smileMapper = smileMapper;
    this.validator = validator;
  }

  @Override
  public void configure(Binder binder)
  {
    binder.install(new DruidGuiceExtensions());
    binder.bind(Properties.class).toInstance(properties);
    binder.bind(ObjectMapper.class).to(Key.get(ObjectMapper.class, Json.class));
    binder.bind(Validator.class).toInstance(validator);
    binder.bind(JsonConfigurator.class);

    // Register frame deserializers.
    getWireTransferableDeserializerBinder(binder)
        .addBinding(StringUtils.toUtf8ByteBuffer(FrameWireTransferable.TYPE))
        .to(FrameWireTransferable.Deserializer.class);
  }

  @Provides @LazySingleton @Json
  public ObjectMapper getJsonMapper(
      final Injector injector,
      final Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers
  )
  {
    setupJackson(injector, jsonMapper, wtDeserializers, isUseLegacyFrameSerialization());
    return jsonMapper;
  }

  @Provides @LazySingleton @JsonNonNull
  public ObjectMapper getJsonMapperOnlyNonNullValueSerialization(
      final Injector injector,
      final Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers
  )
  {
    setupJackson(injector, jsonMapperOnlyNonNullValueSerialization, wtDeserializers, isUseLegacyFrameSerialization());
    return jsonMapperOnlyNonNullValueSerialization;
  }

  @Provides @LazySingleton @Smile
  public ObjectMapper getSmileMapper(
      final Injector injector,
      final Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers
  )
  {
    setupJackson(injector, smileMapper, wtDeserializers, isUseLegacyFrameSerialization());
    return smileMapper;
  }

  /**
   * Whether to use the legacy serialization format (true) or {@link FrameWireTransferable} (false) when serializing
   * {@link FrameRowsAndColumns} with Jackson. Deserialization supports both formats regardless of the setting of
   * this property.
   */
  private boolean isUseLegacyFrameSerialization()
  {
    return Boolean.parseBoolean(
        properties.getProperty(
            PROPERTY_RAC_LEGACY_FRAME_SERIALIZATION,
            String.valueOf(WireTransferableContext.DEFAULT_LEGACY_FRAME_SERIALIZATION)
        )
    );
  }

  @Provides @LazySingleton @Smile
  public WireTransferable.ConcreteDeserializer getConcreteDeserializer(
      @Smile final ObjectMapper smileMapper,
      final Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers
  )
  {
    return new WireTransferable.ConcreteDeserializer(smileMapper, wtDeserializers);
  }

  @Provides @LazySingleton
  public WireTransferableContext getWireTransferableContext(
      @Smile final ObjectMapper smileMapper,
      @Smile final WireTransferable.ConcreteDeserializer concreteDeserializer
  )
  {
    return new WireTransferableContext(smileMapper, concreteDeserializer, isUseLegacyFrameSerialization());
  }

  @Provides
  @LazySingleton
  @Deterministic
  public ObjectMapper getSortedMapper(
      Injector injector,
      Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers
  )
  {
    final ObjectMapper sortedMapper = new DefaultObjectMapper();
    sortedMapper.configure(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS, true);
    sortedMapper.configure(MapperFeature.SORT_PROPERTIES_ALPHABETICALLY, true);
    setupJackson(injector, sortedMapper, wtDeserializers, isUseLegacyFrameSerialization());
    return sortedMapper;
  }


  public static void setupJackson(
      final Injector injector,
      final ObjectMapper mapper,
      final Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers,
      final boolean useLegacyFrameSerialization
  )
  {
    mapper.setInjectableValues(new GuiceInjectableValues(injector));
    setupAnnotationIntrospector(mapper, new GuiceAnnotationIntrospector());

    attachWireTransferables(mapper, wtDeserializers, useLegacyFrameSerialization);
  }

  /**
   * Attach {@link RowsAndColumnsSerializer} and {@link RowsAndColumnsDeserializer} to an {@link ObjectMapper}.
   */
  public static void attachWireTransferables(
      ObjectMapper mapper,
      Map<ByteBuffer, WireTransferable.Deserializer> wtDeserializers,
      boolean useLegacyFrameSerialization
  )
  {
    final WireTransferableContext wtContext = new WireTransferableContext(
        mapper,
        new WireTransferable.ConcreteDeserializer(mapper, wtDeserializers),
        useLegacyFrameSerialization
    );
    mapper.registerModule(
        new SimpleModule("wireTransferableSerde")
            .addDeserializer(RowsAndColumns.class, new RowsAndColumnsDeserializer(wtContext))
            .addSerializer(RowsAndColumns.class, new RowsAndColumnsSerializer(wtContext))
    );
  }

  public static void setupAnnotationIntrospector(
      final ObjectMapper mapper,
      final AnnotationIntrospector annotationIntrospector
  )
  {
    mapper.setAnnotationIntrospectors(
        new AnnotationIntrospectorPair(
            annotationIntrospector,
            mapper.getSerializationConfig().getAnnotationIntrospector()
        ),
        new AnnotationIntrospectorPair(
            annotationIntrospector,
            mapper.getDeserializationConfig().getAnnotationIntrospector()
        )
    );
  }
}
