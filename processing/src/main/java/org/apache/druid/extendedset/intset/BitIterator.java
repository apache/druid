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

package org.apache.druid.extendedset.intset;

import java.util.NoSuchElementException;

// Based on the ConciseSet implementation by Alessandro Colantonio
public final class BitIterator implements IntSet.IntIterator
{
  private final ImmutableConciseSet immutableConciseSet;

  private boolean literalAndZeroFill;
  private int nextIndex = 0;
  private int nextOffset = 0;
  private int next;

  BitIterator(ImmutableConciseSet immutableConciseSet)
  {
    this.immutableConciseSet = immutableConciseSet;
    nextWord();
    next = advance();
  }

  private BitIterator(
      ImmutableConciseSet immutableConciseSet,
      boolean literalAndZeroFill,
      int nextIndex,
      int nextOffset,
      int next,
      int literalAndZeroFillLen,
      int literalAndZeroFillCurrent,
      int oneFillFirstInt,
      int oneFillLastInt,
      int oneFillCurrent,
      int oneFillException
  )
  {
    this.immutableConciseSet = immutableConciseSet;
    this.literalAndZeroFill = literalAndZeroFill;
    this.nextIndex = nextIndex;
    this.nextOffset = nextOffset;
    this.next = next;
    this.literalAndZeroFillLen = literalAndZeroFillLen;
    this.literalAndZeroFillCurrent = literalAndZeroFillCurrent;
    this.oneFillFirstInt = oneFillFirstInt;
    this.oneFillLastInt = oneFillLastInt;
    this.oneFillCurrent = oneFillCurrent;
    this.oneFillException = oneFillException;
  }

  @Override
  public boolean hasNext()
  {
    return next >= 0;
  }

  private int advance()
  {
    int wordExpanderNext;
    while ((wordExpanderNext = wordExpanderAdvance()) < 0) {
      if (nextIndex > immutableConciseSet.lastWordIndex) {
        return -1;
      }
      nextWord();
    }
    return wordExpanderNext;
  }

  @Override
  public int next()
  {
    int prev = next;
    if (prev >= 0) {
      next = advance();
      return prev;
    } else {
      throw new NoSuchElementException();
    }
  }

  @Override
  public void skipAllBefore(int element)
  {
    if (element <= next) {
      return;
    }
    while (true) {
      wordExpanderSkipAllBefore(element);
      int wordExpanderNext = wordExpanderAdvance();
      if (wordExpanderNext >= 0) {
        next = wordExpanderNext;
        return;
      }
      if (nextIndex > immutableConciseSet.lastWordIndex) {
        next = -1;
        return;
      }
      nextWord();
    }
  }

  @Override
  public IntSet.IntIterator clone()
  {
    BitIterator clone = new BitIterator(
            immutableConciseSet,
            literalAndZeroFill,
            nextIndex,
            nextOffset,
            next,
            literalAndZeroFillLen,
            literalAndZeroFillCurrent,
            oneFillFirstInt,
            oneFillLastInt,
            oneFillCurrent,
            oneFillException
    );
    System.arraycopy(
            literalAndZeroFillBuffer,
            0,
            clone.literalAndZeroFillBuffer,
            0,
            literalAndZeroFillBuffer.length
    );
    return clone;
  }

  private void nextWord()
  {
    final int word = immutableConciseSet.words.get(nextIndex++);
    literalAndZeroFill = wordExpanderReset(nextOffset, word);

    // prepare next offset
    if (ConciseSetUtils.isLiteral(word)) {
      nextOffset += ConciseSetUtils.MAX_LITERAL_LENGTH;
    } else {
      nextOffset += ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(word) + 1);
    }
  }

  private int wordExpanderAdvance()
  {
    return literalAndZeroFill ? literalAndZeroFillAdvance() : oneFillAdvance();
  }

  private void wordExpanderSkipAllBefore(int i)
  {
    if (literalAndZeroFill) {
      literalAndZeroFillSkipAllBefore(i);
    } else {
      oneFillSkipAllBefore(i);
    }
  }

  private boolean wordExpanderReset(int offset, int word)
  {
    if (ConciseSetUtils.isLiteral(word)) {
      literalAndZeroFillResetLiteral(offset, word);
      return true;
    } else if (ConciseSetUtils.isZeroSequence(word)) {
      literalAndZeroFillResetZeroSequence(offset, word);
      return true;
    } else {
      // one sequence
      oneFillReset(offset, word);
      return false;
    }
  }

  private final int[] literalAndZeroFillBuffer = new int[ConciseSetUtils.MAX_LITERAL_LENGTH];
  private int literalAndZeroFillLen = 0;
  private int literalAndZeroFillCurrent = 0;

  private int literalAndZeroFillAdvance()
  {
    if (literalAndZeroFillCurrent < literalAndZeroFillLen) {
      return literalAndZeroFillBuffer[literalAndZeroFillCurrent++];
    } else {
      return -1;
    }
  }

  private void literalAndZeroFillSkipAllBefore(int i)
  {
    while (literalAndZeroFillCurrent < literalAndZeroFillLen &&
           literalAndZeroFillBuffer[literalAndZeroFillCurrent] < i) {
      literalAndZeroFillCurrent++;
    }
  }

  private void literalAndZeroFillResetZeroSequence(int offset, int word)
  {
    if (ConciseSetUtils.isSequenceWithNoBits(word)) {
      literalAndZeroFillLen = 0;
      literalAndZeroFillCurrent = 0;
    } else {
      literalAndZeroFillLen = 1;
      literalAndZeroFillBuffer[0] = offset + ((0x3FFFFFFF & word) >>> 25) - 1;
      literalAndZeroFillCurrent = 0;
    }
  }

  private void literalAndZeroFillResetLiteral(int offset, int word)
  {
    literalAndZeroFillLen = 0;
    for (int i = 0; i < ConciseSetUtils.MAX_LITERAL_LENGTH; i++) {
      if ((word & (1 << i)) != 0) {
        literalAndZeroFillBuffer[literalAndZeroFillLen++] = offset + i;
      }
    }
    literalAndZeroFillCurrent = 0;
  }

  private int oneFillFirstInt = 1;
  private int oneFillLastInt = -1;
  private int oneFillCurrent = 0;
  private int oneFillException = -1;

  private int oneFillAdvance()
  {
    int oneFillCurrent = this.oneFillCurrent;
    if (oneFillCurrent < oneFillLastInt) {
      return oneFillDoAdvance(oneFillCurrent);
    } else {
      return -1;
    }
  }

  private int oneFillDoAdvance(int oneFillCurrent)
  {
    oneFillCurrent++;
    if (oneFillCurrent == oneFillException) {
      oneFillCurrent++;
    }
    this.oneFillCurrent = oneFillCurrent;
    return oneFillCurrent;
  }

  private void oneFillSkipAllBefore(int i)
  {
    if (i <= oneFillCurrent) {
      return;
    }
    oneFillCurrent = i - 1;
  }

  private void oneFillReset(int offset, int word)
  {
    if (!ConciseSetUtils.isOneSequence(word)) {
      throw new RuntimeException("NOT a sequence of ones!");
    }
    oneFillFirstInt = offset;
    oneFillLastInt = offset + ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(word) + 1) - 1;

    oneFillException = offset + ((0x3FFFFFFF & word) >>> 25) - 1;
    if (oneFillException == oneFillFirstInt) {
      oneFillFirstInt++;
    }
    if (oneFillException == oneFillLastInt) {
      oneFillLastInt--;
    }

    oneFillCurrent = oneFillFirstInt - 1;
  }
}
