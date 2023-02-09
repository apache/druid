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

package org.apache.druid.msq.statistics;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import org.apache.druid.frame.key.RowKey;

import java.io.IOException;
import java.util.Comparator;
import java.util.Optional;

/**
 * See {@link DelegateOrMinKeyCollector} for details.
 */
public class DelegateOrMinKeyCollectorFactory<TDelegate extends KeyCollector<TDelegate>, TSnapshot extends KeyCollectorSnapshot>
    implements KeyCollectorFactory<DelegateOrMinKeyCollector<TDelegate>, DelegateOrMinKeyCollectorSnapshot<TSnapshot>>
{
  private final Comparator<RowKey> comparator;
  private final KeyCollectorFactory<TDelegate, TSnapshot> delegateFactory;

  public DelegateOrMinKeyCollectorFactory(
      final Comparator<RowKey> comparator,
      final KeyCollectorFactory<TDelegate, TSnapshot> delegateFactory
  )
  {
    this.comparator = comparator;
    this.delegateFactory = delegateFactory;
  }

  @Override
  public DelegateOrMinKeyCollector<TDelegate> newKeyCollector()
  {
    return new DelegateOrMinKeyCollector<>(comparator, delegateFactory.newKeyCollector(), null);
  }

  @Override
  public JsonDeserializer<DelegateOrMinKeyCollectorSnapshot<TSnapshot>> snapshotDeserializer()
  {
    final JsonDeserializer<TSnapshot> delegateDeserializer = delegateFactory.snapshotDeserializer();

    return new JsonDeserializer<DelegateOrMinKeyCollectorSnapshot<TSnapshot>>()
    {
      @Override
      public DelegateOrMinKeyCollectorSnapshot<TSnapshot> deserialize(JsonParser jp, DeserializationContext ctxt)
          throws IOException
      {
        TSnapshot delegateSnapshot = null;
        RowKey minKey = null;

        if (!jp.isExpectedStartObjectToken()) {
          ctxt.reportWrongTokenException(this, JsonToken.START_OBJECT, null);
        }

        JsonToken token;

        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
          if (token != JsonToken.FIELD_NAME) {
            ctxt.reportWrongTokenException(this, JsonToken.FIELD_NAME, null);
          }

          final String fieldName = jp.getText();
          jp.nextToken();

          if (DelegateOrMinKeyCollectorSnapshot.FIELD_SNAPSHOT.equals(fieldName)) {
            delegateSnapshot = delegateDeserializer.deserialize(jp, ctxt);
          } else if (DelegateOrMinKeyCollectorSnapshot.FIELD_MIN_KEY.equals(fieldName)) {
            minKey = jp.readValueAs(RowKey.class);
          }
        }

        return new DelegateOrMinKeyCollectorSnapshot<>(delegateSnapshot, minKey);
      }
    };
  }

  @Override
  public DelegateOrMinKeyCollectorSnapshot<TSnapshot> toSnapshot(final DelegateOrMinKeyCollector<TDelegate> collector)
  {
    final RowKey minKeyForSnapshot;

    if (!collector.getDelegate().isPresent() && !collector.isEmpty()) {
      minKeyForSnapshot = collector.minKey();
    } else {
      minKeyForSnapshot = null;
    }

    return new DelegateOrMinKeyCollectorSnapshot<>(
        collector.getDelegate().map(delegateFactory::toSnapshot).orElse(null),
        minKeyForSnapshot
    );
  }

  @Override
  public DelegateOrMinKeyCollector<TDelegate> fromSnapshot(final DelegateOrMinKeyCollectorSnapshot<TSnapshot> snapshot)
  {
    return new DelegateOrMinKeyCollector<>(
        comparator,
        Optional.ofNullable(snapshot.getSnapshot()).map(delegateFactory::fromSnapshot).orElse(null),
        snapshot.getMinKey()
    );
  }
}
