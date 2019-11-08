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

package org.apache.druid.indexing.overlord.sampler;

import org.apache.druid.client.cache.Cache;
import org.apache.druid.data.input.ByteBufferInputRowParser;
import org.apache.druid.data.input.Firehose;
import org.apache.druid.data.input.FirehoseFactory;
import org.apache.druid.data.input.InputRow;
import org.apache.druid.data.input.InputRowPlusRaw;
import org.apache.druid.data.input.impl.InputRowParser;
import org.apache.druid.data.input.impl.StringInputRowParser;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.parsers.ParseException;
import org.apache.druid.java.util.emitter.EmittingLogger;

import javax.annotation.Nullable;
import javax.inject.Inject;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SamplerCache
{
  private static final EmittingLogger log = new EmittingLogger(SamplerCache.class);
  private static final String NAMESPACE = "sampler";

  private final Cache cache;

  @Inject
  public SamplerCache(Cache cache)
  {
    this.cache = cache;
  }

  @Nullable
  public String put(String key, Collection<byte[]> values)
  {
    if (values == null) {
      return null;
    }

    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
         ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(new ArrayList<>(values));
      cache.put(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)), baos.toByteArray());
      return key;
    }
    catch (IOException e) {
      log.warn(e, "Exception while serializing to sampler cache");
      return null;
    }
  }

  @Nullable
  public FirehoseFactory getAsFirehoseFactory(String key, InputRowParser parser)
  {
    if (!(parser instanceof ByteBufferInputRowParser)) {
      log.warn("SamplerCache expects a ByteBufferInputRowParser");
      return null;
    }

    Collection<byte[]> data = get(key);
    if (data == null) {
      return null;
    }

    return new FirehoseFactory<ByteBufferInputRowParser>()
    {
      @Override
      public Firehose connect(ByteBufferInputRowParser parser, @Nullable File temporaryDirectory)
      {
        return new SamplerCacheFirehose(parser, data);
      }
    };
  }

  @Nullable
  private Collection<byte[]> get(String key)
  {
    byte[] data = cache.get(new Cache.NamedKey(NAMESPACE, StringUtils.toUtf8(key)));
    if (data == null) {
      return null;
    }

    try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
         ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (ArrayList) ois.readObject();
    }
    catch (Exception e) {
      log.warn(e, "Exception while deserializing from sampler cache");
      return null;
    }
  }

  public static class SamplerCacheFirehose implements Firehose
  {
    private final ByteBufferInputRowParser parser;
    private final Iterator<byte[]> it;

    public SamplerCacheFirehose(ByteBufferInputRowParser parser, Collection<byte[]> data)
    {
      this.parser = parser;
      this.it = data != null ? data.iterator() : Collections.emptyIterator();

      if (parser instanceof StringInputRowParser) {
        ((StringInputRowParser) parser).startFileFromBeginning();
      }
    }

    @Override
    public boolean hasMore()
    {
      return it.hasNext();
    }

    @Nullable
    @Override
    public InputRow nextRow()
    {
      if (!hasMore()) {
        throw new NoSuchElementException();
      }

      List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(it.next()));
      return rows.isEmpty() ? null : rows.get(0);
    }

    @Override
    public InputRowPlusRaw nextRowWithRaw()
    {
      if (!hasMore()) {
        throw new NoSuchElementException();
      }

      byte[] raw = it.next();

      try {
        List<InputRow> rows = parser.parseBatch(ByteBuffer.wrap(raw));
        return InputRowPlusRaw.of(rows.isEmpty() ? null : rows.get(0), raw);
      }
      catch (ParseException e) {
        return InputRowPlusRaw.of(raw, e);
      }
    }

    @Override
    public void close()
    {
    }
  }
}
