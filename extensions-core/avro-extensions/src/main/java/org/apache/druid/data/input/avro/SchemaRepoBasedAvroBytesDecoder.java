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

package org.apache.druid.data.input.avro;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.ByteBufferInputStream;
import org.apache.druid.data.input.schemarepo.SubjectAndIdConverter;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.schemarepo.Repository;
import org.schemarepo.api.TypedSchemaRepository;
import org.schemarepo.api.converter.AvroSchemaConverter;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Objects;

public class SchemaRepoBasedAvroBytesDecoder<SUBJECT, ID> implements AvroBytesDecoder
{
  private final TypedSchemaRepository<ID, Schema, SUBJECT> typedRepository;
  private final SubjectAndIdConverter<SUBJECT, ID> subjectAndIdConverter;
  private final Repository schemaRepository;

  @JsonCreator
  public SchemaRepoBasedAvroBytesDecoder(
      @JsonProperty("subjectAndIdConverter") SubjectAndIdConverter<SUBJECT, ID> subjectAndIdConverter,
      @JsonProperty("schemaRepository") Repository schemaRepository
  )
  {
    this.subjectAndIdConverter = subjectAndIdConverter;
    this.schemaRepository = schemaRepository;
    this.typedRepository = new TypedSchemaRepository<>(
        schemaRepository,
        subjectAndIdConverter.getIdConverter(),
        new AvroSchemaConverter(false),
        subjectAndIdConverter.getSubjectConverter()
    );
  }

  @JsonProperty
  public Repository getSchemaRepository()
  {
    return schemaRepository;
  }

  @JsonProperty
  public SubjectAndIdConverter<SUBJECT, ID> getSubjectAndIdConverter()
  {
    return subjectAndIdConverter;
  }

  @Override
  public GenericRecord parse(ByteBuffer bytes)
  {
    Pair<SUBJECT, ID> subjectAndId = subjectAndIdConverter.getSubjectAndId(bytes);
    Schema schema = typedRepository.getSchema(subjectAndId.lhs, subjectAndId.rhs);
    DatumReader<GenericRecord> reader = new GenericDatumReader<>(schema);
    try (ByteBufferInputStream inputStream = new ByteBufferInputStream(Collections.singletonList(bytes))) {
      return reader.read(null, DecoderFactory.get().binaryDecoder(inputStream, null));
    }
    catch (EOFException eof) {
      // waiting for avro v1.9.0 (#AVRO-813)
      throw new ParseException(
          eof,
          "Avro's unnecessary EOFException, detail: [%s]",
          "https://issues.apache.org/jira/browse/AVRO-813"
      );
    }
    catch (IOException e) {
      throw new ParseException(e, "Fail to decode avro message!");
    }
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    SchemaRepoBasedAvroBytesDecoder<?, ?> that = (SchemaRepoBasedAvroBytesDecoder<?, ?>) o;

    if (!Objects.equals(subjectAndIdConverter, that.subjectAndIdConverter)) {
      return false;
    }
    return Objects.equals(schemaRepository, that.schemaRepository);
  }

  @Override
  public int hashCode()
  {
    int result = subjectAndIdConverter != null ? subjectAndIdConverter.hashCode() : 0;
    result = 31 * result + (schemaRepository != null ? schemaRepository.hashCode() : 0);
    return result;
  }
}
