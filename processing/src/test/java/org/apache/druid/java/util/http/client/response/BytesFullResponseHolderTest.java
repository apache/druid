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

package org.apache.druid.java.util.http.client.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

import java.util.Objects;

public class BytesFullResponseHolderTest {

    ObjectMapper objectMapper = new DefaultObjectMapper();

    @Test
    public void testDeserialize() throws Exception {
        final ResponseObject payload = new ResponseObject("payload123");

        final DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);

        final BytesFullResponseHolder target = Mockito.spy(new BytesFullResponseHolder(response));
        target.addChunk(objectMapper.writeValueAsBytes(payload));

        final ResponseObject deserialize = target.deserialize(objectMapper, new TypeReference<ResponseObject>() {
        });

        Assert.assertEquals(payload, deserialize);
        Mockito.verify(target, Mockito.times(1)).deserialize(ArgumentMatchers.any(), ArgumentMatchers.any());
    }

    static class ResponseObject {
        String payload;

        @JsonCreator
        public ResponseObject(@JsonProperty("payload") String payload) {
            this.payload = payload;
        }

        @JsonProperty("payload")
        public String getPayload() {
            return payload;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ResponseObject that = (ResponseObject) o;
            return Objects.equals(payload, that.payload);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(payload);
        }
    }
}