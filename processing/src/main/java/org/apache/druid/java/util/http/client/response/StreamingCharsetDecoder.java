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

import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;

/**
 * Decodes a stream of {@link ByteBuf} chunks into characters appended to a caller-owned
 * {@link StringBuilder}. Handles the case where a multi-byte character straddles a chunk boundary
 * by holding partial-character bytes over from one call to the next; without this, calling
 * {@code ByteBuf.toString(charset)} per chunk turns a split character into two replacement characters
 * (one on each side of the boundary).
 *
 * <p>Not thread-safe. Uses {@link CodingErrorAction#REPLACE} for both malformed and unmappable
 * inputs, matching {@code ByteBuf.toString(charset)}'s behaviour for genuinely bad bytes.
 */
class StreamingCharsetDecoder
{
  private static final ByteBuffer EMPTY = ByteBuffer.allocate(0);

  private final StringBuilder target;
  private final CharsetDecoder decoder;
  private final CharBuffer scratch = CharBuffer.allocate(1024);
  private ByteBuffer leftover;

  StreamingCharsetDecoder(StringBuilder target, Charset charset)
  {
    this.target = target;
    this.decoder = charset.newDecoder()
                          .onMalformedInput(CodingErrorAction.REPLACE)
                          .onUnmappableCharacter(CodingErrorAction.REPLACE);
  }

  /**
   * Append the bytes of a chunk. Bytes that do not yet form a complete character (e.g. the leading
   * byte of a two-byte UTF-8 sequence that arrived at the very end of this chunk) are held over and
   * joined with the head of the next chunk.
   */
  void append(ByteBuf chunk)
  {
    // Iterating nioBuffers rather than calling nioBuffer() handles composite buffers too, which the
    // single-buffer accessor throws UnsupportedOperationException for.
    for (ByteBuffer nio : chunk.nioBuffers()) {
      decodeWithLeftover(nio, false);
    }
  }

  /**
   * Signal end-of-input so any trailing bytes that could not yet form a character emit replacement
   * characters rather than silently disappearing.
   */
  void finish()
  {
    decodeWithLeftover(EMPTY, true);
    scratch.clear();
    decoder.flush(scratch);
    scratch.flip();
    target.append(scratch);
  }

  private void decodeWithLeftover(ByteBuffer incoming, boolean endOfInput)
  {
    final ByteBuffer toDecode;
    if (leftover != null && leftover.hasRemaining()) {
      // Prepend the bytes held back from the previous chunk so they can combine with the new head.
      toDecode = ByteBuffer.allocate(leftover.remaining() + incoming.remaining());
      toDecode.put(leftover).put(incoming).flip();
      leftover = null;
    } else {
      toDecode = incoming;
    }
    runDecode(toDecode, endOfInput);
    if (!endOfInput && toDecode.hasRemaining()) {
      // Partial-character bytes at the tail; hold them for the next chunk.
      leftover = ByteBuffer.allocate(toDecode.remaining());
      leftover.put(toDecode);
      leftover.flip();
    }
  }

  private void runDecode(ByteBuffer bytes, boolean endOfInput)
  {
    while (true) {
      scratch.clear();
      final CoderResult result = decoder.decode(bytes, scratch, endOfInput);
      scratch.flip();
      target.append(scratch);
      if (result.isUnderflow()) {
        break;
      }
      // Overflow: scratch was full; loop and drain more.
    }
  }
}
