/*
 * Druid - a distributed column store.
 * Copyright (C) 2012  Metamarkets Group Inc.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * as published by the Free Software Foundation; either version 2
 * of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */

package com.metamx.druid.jackson;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.ToStringSerializer;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.google.common.base.Throwables;
import com.metamx.common.Granularity;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;

import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.TimeZone;

import gnu.trove.map.hash.TIntByteHashMap;
import org.apache.commons.codec.binary.Base64;
import java.nio.ByteBuffer;

/**
 */
public class DefaultObjectMapper extends ObjectMapper {
	public DefaultObjectMapper() {
		this(null);
	}

	public DefaultObjectMapper(JsonFactory factory) {
		super(factory);
		SimpleModule serializerModule = new SimpleModule(
				"Druid default serializers", new Version(1, 0, 0, null));
		JodaStuff.register(serializerModule);
		serializerModule.addDeserializer(Granularity.class,
				new JsonDeserializer<Granularity>() {
					@Override
					public Granularity deserialize(JsonParser jp,
							DeserializationContext ctxt) throws IOException {
						return Granularity.valueOf(jp.getText().toUpperCase());
					}
				});
		serializerModule.addDeserializer(DateTimeZone.class,
				new JsonDeserializer<DateTimeZone>() {
					@Override
					public DateTimeZone deserialize(JsonParser jp,
							DeserializationContext ctxt) throws IOException {
						String tzId = jp.getText();
						try {
							return DateTimeZone.forID(tzId);
						} catch (IllegalArgumentException e) {
							// also support Java timezone strings
							return DateTimeZone.forTimeZone(TimeZone
									.getTimeZone(tzId));
						}
					}
				});
		serializerModule.addSerializer(DateTimeZone.class,
				new JsonSerializer<DateTimeZone>() {
					@Override
					public void serialize(DateTimeZone dateTimeZone,
							JsonGenerator jsonGenerator,
							SerializerProvider serializerProvider)
							throws IOException, JsonProcessingException {
						jsonGenerator.writeString(dateTimeZone.getID());
					}
				});
		serializerModule.addSerializer(Sequence.class,
				new JsonSerializer<Sequence>() {
					@Override
					public void serialize(Sequence value,
							final JsonGenerator jgen,
							SerializerProvider provider) throws IOException,
							JsonProcessingException {
						jgen.writeStartArray();
						value.accumulate(null, new Accumulator() {
							@Override
							public Object accumulate(Object o, Object o1) {
								try {
									jgen.writeObject(o1);
								} catch (IOException e) {
									throw Throwables.propagate(e);
								}
								return o;
							}
						});
						jgen.writeEndArray();
					}
				});

		serializerModule.addSerializer(ByteOrder.class,
				ToStringSerializer.instance);
		serializerModule.addDeserializer(ByteOrder.class,
				new JsonDeserializer<ByteOrder>() {
					@Override
					public ByteOrder deserialize(JsonParser jp,
							DeserializationContext ctxt) throws IOException,
							JsonProcessingException {
						if (ByteOrder.BIG_ENDIAN.toString()
								.equals(jp.getText())) {
							return ByteOrder.BIG_ENDIAN;
						}
						return ByteOrder.LITTLE_ENDIAN;
					}
				});

		serializerModule.addDeserializer(TIntByteHashMap.class,
				new JsonDeserializer<TIntByteHashMap>() {
					@Override
					public TIntByteHashMap deserialize(JsonParser jp,
							DeserializationContext ctxt) throws IOException {
						byte[] ibmapByte = Base64.decodeBase64(jp.getText());

						ByteBuffer buffer = ByteBuffer.wrap(ibmapByte);
						int keylength = buffer.getInt();
						int valuelength = buffer.getInt();
						if (keylength == 0) {
							return (new TIntByteHashMap());
						}
						int[] keys = new int[keylength];
						byte[] values = new byte[valuelength];

						for (int i = 0; i < keylength; i++) {
							keys[i] = buffer.getInt();
						}
						buffer.get(values);

						return (new TIntByteHashMap(keys, values));
					}
				});

		serializerModule.addSerializer(TIntByteHashMap.class,
				new JsonSerializer<TIntByteHashMap>() {
					@Override
					public void serialize(TIntByteHashMap ibmap,
							JsonGenerator jsonGenerator,
							SerializerProvider serializerProvider)
							throws IOException, JsonProcessingException {
						int[] indexesResult = ibmap.keys();
						byte[] valueResult = ibmap.values();
						ByteBuffer buffer = ByteBuffer
								.allocate(4 * indexesResult.length
										+ valueResult.length + 8);
						byte[] result = new byte[4 * indexesResult.length
								+ valueResult.length + 8];
						buffer.putInt((int) indexesResult.length);
						buffer.putInt((int) valueResult.length);
						for (int i = 0; i < indexesResult.length; i++) {
							buffer.putInt(indexesResult[i]);
						}

						buffer.put(valueResult);
						buffer.flip();
						buffer.get(result);
						String str = Base64.encodeBase64String(result);
						jsonGenerator.writeString(str);
					}
				});

		registerModule(serializerModule);
		registerModule(new GuavaModule());

		configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		configure(MapperFeature.AUTO_DETECT_GETTERS, false);
		// configure(MapperFeature.AUTO_DETECT_CREATORS, false);
		// https://github.com/FasterXML/jackson-databind/issues/170
		configure(MapperFeature.AUTO_DETECT_FIELDS, false);
		configure(MapperFeature.AUTO_DETECT_IS_GETTERS, false);
		configure(MapperFeature.AUTO_DETECT_SETTERS, false);
		configure(SerializationFeature.INDENT_OUTPUT, false);
	}
}
