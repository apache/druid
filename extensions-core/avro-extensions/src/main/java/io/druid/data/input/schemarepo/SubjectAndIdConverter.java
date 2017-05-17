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
package io.druid.data.input.schemarepo;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.druid.java.util.common.Pair;
import org.schemarepo.api.converter.Converter;

import java.nio.ByteBuffer;

/**
 * Schema Repository is a registry service, you can register a string schema which gives back an schema id for it,
 * or lookup the schema with the schema id.
 * <p>
 * In order to get the "latest" schema or handle compatibility enforcement on changes there has to be some way to group
 * a set of schemas together and reason about the ordering of changes over these. <i>Subject</i> is introduced as
 * the formal notion of <i>group</i>, defined as an ordered collection of mutually compatible schemas, according to <a href="https://issues.apache.org/jira/browse/AVRO-1124?focusedCommentId=13503967&page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel#comment-13503967">
 * Scott Carey on AVRO-1124</a>.
 * <p>
 * So you can register an string schema to a specific subject, get an schema id, and then query the schema using the
 * subject and schema id pair. Working with Kafka and Avro, it's intuitive that using Kafka topic as subject name and an
 * incrementing integer as schema id, serialize and attach them to the message payload, or extract and deserialize from
 * message payload, which is implemented as {@link Avro1124SubjectAndIdConverter}.
 * <p>
 * You can implement your own SubjectAndIdConverter based on your scenario, such as using canonical name of avro schema
 * as subject name and incrementing short integer which serialized using varint.
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type", defaultImpl = Avro1124SubjectAndIdConverter.class)
@JsonSubTypes(value = {
    @JsonSubTypes.Type(name = "avro_1124", value = Avro1124SubjectAndIdConverter.class)
})
public interface SubjectAndIdConverter<SUBJECT, ID>
{

  Pair<SUBJECT, ID> getSubjectAndId(ByteBuffer payload);

  void putSubjectAndId(SUBJECT subject, ID id, ByteBuffer payload);

  Converter<SUBJECT> getSubjectConverter();

  Converter<ID> getIdConverter();
}
