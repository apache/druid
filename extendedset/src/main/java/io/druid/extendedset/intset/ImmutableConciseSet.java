/*
* Copyright 2012 Metamarkets Group Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package io.druid.extendedset.intset;


import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.MinMaxPriorityQueue;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.primitives.Ints;
import io.druid.extendedset.utilities.IntList;

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class ImmutableConciseSet
{
  private final static int CHUNK_SIZE = 10000;
  private final IntBuffer words;
  private final int lastWordIndex;
  private final int size;

  public ImmutableConciseSet()
  {
    this.words = null;
    this.lastWordIndex = -1;
    this.size = 0;
  }

  public ImmutableConciseSet(ByteBuffer byteBuffer)
  {
    this.words = byteBuffer.asIntBuffer();
    this.lastWordIndex = words.capacity() - 1;
    this.size = calcSize();
  }

  public ImmutableConciseSet(IntBuffer buffer)
  {
    this.words = buffer;
    this.lastWordIndex = (words == null || buffer.capacity() == 0) ? -1 : words.capacity() - 1;
    this.size = calcSize();
  }

  public static ImmutableConciseSet newImmutableFromMutable(ConciseSet conciseSet)
  {
    if (conciseSet == null || conciseSet.isEmpty()) {
      return new ImmutableConciseSet();
    }
    return new ImmutableConciseSet(IntBuffer.wrap(conciseSet.getWords()));
  }

  public static int compareInts(int x, int y)
  {
    return (x < y) ? -1 : ((x == y) ? 0 : 1);
  }

  public static ImmutableConciseSet union(ImmutableConciseSet... sets)
  {
    return union(Arrays.asList(sets));
  }

  public static ImmutableConciseSet union(Iterable<ImmutableConciseSet> sets)
  {
    return union(sets.iterator());
  }

  public static ImmutableConciseSet union(Iterator<ImmutableConciseSet> sets)
  {
    ImmutableConciseSet partialResults = doUnion(Iterators.limit(sets, CHUNK_SIZE));
    while (sets.hasNext()) {
      final UnmodifiableIterator<ImmutableConciseSet> partialIter = Iterators.singletonIterator(partialResults);
      partialResults = doUnion(Iterators.<ImmutableConciseSet>concat(partialIter, Iterators.limit(sets, CHUNK_SIZE)));
    }
    return partialResults;
  }

  public static ImmutableConciseSet intersection(ImmutableConciseSet... sets)
  {
    return intersection(Arrays.asList(sets));
  }

  public static ImmutableConciseSet intersection(Iterable<ImmutableConciseSet> sets)
  {
    return intersection(sets.iterator());
  }

  public static ImmutableConciseSet intersection(Iterator<ImmutableConciseSet> sets)
  {
    ImmutableConciseSet partialResults = doIntersection(Iterators.limit(sets, CHUNK_SIZE));
    while (sets.hasNext()) {
      final UnmodifiableIterator<ImmutableConciseSet> partialIter = Iterators.singletonIterator(partialResults);
      partialResults = doIntersection(
          Iterators.<ImmutableConciseSet>concat(Iterators.limit(sets, CHUNK_SIZE), partialIter)
      );
    }
    return partialResults;
  }

  public static ImmutableConciseSet complement(ImmutableConciseSet set)
  {
    return doComplement(set);
  }

  public static ImmutableConciseSet complement(ImmutableConciseSet set, int length)
  {
    if (length <= 0) {
      return new ImmutableConciseSet();
    }

    // special case when the set is empty and we need a concise set of ones
    if (set == null || set.isEmpty()) {
      final int leftoverBits = length % 31;
      final int onesBlocks = length / 31;
      final int[] words;
      if (onesBlocks > 0) {
        if (leftoverBits > 0) {
          words = new int[]{
              ConciseSetUtils.SEQUENCE_BIT | (onesBlocks - 1),
              ConciseSetUtils.onesUntil(leftoverBits)
          };
        } else {
          words = new int[]{
              ConciseSetUtils.SEQUENCE_BIT | (onesBlocks - 1)
          };
        }
      } else {
        if (leftoverBits > 0) {
          words = new int[]{ConciseSetUtils.onesUntil(leftoverBits)};
        } else {
          words = new int[]{};
        }
      }
      ConciseSet newSet = new ConciseSet(words, false);
      return ImmutableConciseSet.newImmutableFromMutable(newSet);
    }

    IntList retVal = new IntList();
    int endIndex = length - 1;

    int wordsWalked = 0;
    int last = 0;

    WordIterator iter = set.newWordIterator();

    while (iter.hasNext()) {
      int word = iter.next();
      wordsWalked = iter.wordsWalked;
      if (ConciseSetUtils.isLiteral(word)) {
        retVal.add(ConciseSetUtils.ALL_ZEROS_LITERAL | ~word);
      } else {
        retVal.add(ConciseSetUtils.SEQUENCE_BIT ^ word);
      }
    }

    last = set.getLast();

    int distFromLastWordBoundary = ConciseSetUtils.maxLiteralLengthModulus(last);
    int distToNextWordBoundary = ConciseSetUtils.MAX_LITERAL_LENGTH - distFromLastWordBoundary - 1;
    last = (last < 0) ? 0 : last + distToNextWordBoundary;

    int diff = endIndex - last;
    // only append a new literal when the end index is beyond the current word
    if (diff > 0) {
      // first check if the difference can be represented in 31 bits
      if (diff <= ConciseSetUtils.MAX_LITERAL_LENGTH) {
        retVal.add(ConciseSetUtils.ALL_ONES_LITERAL);
      } else {
        // create a fill from last set bit to endIndex for number of 31 bit blocks minus one
        int endIndexWordCount = ConciseSetUtils.maxLiteralLengthDivision(endIndex);
        retVal.add(ConciseSetUtils.SEQUENCE_BIT | (endIndexWordCount - wordsWalked - 1));
        retVal.add(ConciseSetUtils.ALL_ONES_LITERAL);
      }
    }

    // clear bits after last set value
    int lastWord = retVal.get(retVal.length() - 1);
    if (ConciseSetUtils.isLiteral(lastWord)) {
      lastWord = ConciseSetUtils.clearBitsAfterInLastWord(
          lastWord,
          ConciseSetUtils.maxLiteralLengthModulus(endIndex)
      );
    }

    retVal.set(retVal.length() - 1, lastWord);
    trimZeros(retVal);

    if (retVal.isEmpty()) {
      return new ImmutableConciseSet();
    }
    return compact(new ImmutableConciseSet(IntBuffer.wrap(retVal.toArray())));
  }

  public static ImmutableConciseSet compact(ImmutableConciseSet set)
  {
    IntList retVal = new IntList();
    WordIterator itr = set.newWordIterator();
    while (itr.hasNext()) {
      addAndCompact(retVal, itr.next());
    }
    return new ImmutableConciseSet(IntBuffer.wrap(retVal.toArray()));
  }

  private static void addAndCompact(IntList set, int wordToAdd)
  {
    int length = set.length();
    if (set.isEmpty()) {
      set.add(wordToAdd);
      return;
    }

    int last = set.get(length - 1);

    int newWord = 0;
    if (ConciseSetUtils.isAllOnesLiteral(last)) {
      if (ConciseSetUtils.isAllOnesLiteral(wordToAdd)) {
        newWord = 0x40000001;
      } else if (ConciseSetUtils.isOneSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = wordToAdd + 1;
      }
    } else if (ConciseSetUtils.isOneSequence(last)) {
      if (ConciseSetUtils.isAllOnesLiteral(wordToAdd)) {
        newWord = last + 1;
      } else if (ConciseSetUtils.isOneSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = last + ConciseSetUtils.getSequenceNumWords(wordToAdd);
      }
    } else if (ConciseSetUtils.isAllZerosLiteral(last)) {
      if (ConciseSetUtils.isAllZerosLiteral(wordToAdd)) {
        newWord = 0x00000001;
      } else if (ConciseSetUtils.isZeroSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = wordToAdd + 1;
      }
    } else if (ConciseSetUtils.isZeroSequence(last)) {
      if (ConciseSetUtils.isAllZerosLiteral(wordToAdd)) {
        newWord = last + 1;
      } else if (ConciseSetUtils.isZeroSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = last + ConciseSetUtils.getSequenceNumWords(wordToAdd);
      }
    } else if (ConciseSetUtils.isLiteralWithSingleOneBit(last)) {
      int position = Integer.numberOfTrailingZeros(last) + 1;
      if (ConciseSetUtils.isAllZerosLiteral(wordToAdd)) {
        newWord = 0x00000001 | (position << 25);
      } else if (ConciseSetUtils.isZeroSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = (wordToAdd + 1) | (position << 25);
      }
    } else if (ConciseSetUtils.isLiteralWithSingleZeroBit(last)) {
      int position = Integer.numberOfTrailingZeros(~last) + 1;
      if (ConciseSetUtils.isAllOnesLiteral(wordToAdd)) {
        newWord = 0x40000001 | (position << 25);
      } else if (ConciseSetUtils.isOneSequence(wordToAdd) && ConciseSetUtils.getFlippedBit(wordToAdd) == -1) {
        newWord = (wordToAdd + 1) | (position << 25);
      }
    }

    if (newWord != 0) {
      set.set(length - 1, newWord);
    } else {
      set.add(wordToAdd);
    }
  }

  private static ImmutableConciseSet doUnion(Iterator<ImmutableConciseSet> sets)
  {
    IntList retVal = new IntList();

    // lhs = current word position, rhs = the iterator
    // Comparison is first by index, then one fills > literals > zero fills
    // one fills are sorted by length (longer one fills have priority)
    // similarily, shorter zero fills have priority
    MinMaxPriorityQueue<WordHolder> theQ = MinMaxPriorityQueue.orderedBy(
        new Comparator<WordHolder>()
        {
          @Override
          public int compare(WordHolder h1, WordHolder h2)
          {
            int w1 = h1.getWord();
            int w2 = h2.getWord();
            int s1 = h1.getIterator().startIndex;
            int s2 = h2.getIterator().startIndex;

            if (s1 != s2) {
              return compareInts(s1, s2);
            }

            if (ConciseSetUtils.isOneSequence(w1)) {
              if (ConciseSetUtils.isOneSequence(w2)) {
                return -compareInts(ConciseSetUtils.getSequenceNumWords(w1), ConciseSetUtils.getSequenceNumWords(w2));
              }
              return -1;
            } else if (ConciseSetUtils.isLiteral(w1)) {
              if (ConciseSetUtils.isOneSequence(w2)) {
                return 1;
              } else if (ConciseSetUtils.isLiteral(w2)) {
                return 0;
              }
              return -1;
            } else {
              if (!ConciseSetUtils.isZeroSequence(w2)) {
                return 1;
              }
              return compareInts(ConciseSetUtils.getSequenceNumWords(w1), ConciseSetUtils.getSequenceNumWords(w2));
            }
          }
        }
    ).create();

    // populate priority queue
    while (sets.hasNext()) {
      ImmutableConciseSet set = sets.next();

      if (set != null && !set.isEmpty()) {
        WordIterator itr = set.newWordIterator();
        theQ.add(new WordHolder(itr.next(), itr));
      }
    }

    int currIndex = 0;

    while (!theQ.isEmpty()) {
      // create a temp list to hold everything that will get pushed back into the priority queue after each run
      List<WordHolder> wordsToAdd = Lists.newArrayList();

      // grab the top element from the priority queue
      WordHolder curr = theQ.poll();
      int word = curr.getWord();
      WordIterator itr = curr.getIterator();

      // if the next word in the queue starts at a different point than where we ended off we need to create a zero gap
      // to fill the space
      if (currIndex < itr.startIndex) {
        addAndCompact(retVal, itr.startIndex - currIndex - 1);
        currIndex = itr.startIndex;
      }

      if (ConciseSetUtils.isOneSequence(word)) {
        // extract a literal from the flip bits of the one sequence
        int flipBitLiteral = ConciseSetUtils.getLiteralFromOneSeqFlipBit(word);

        // advance everything past the longest ones sequence
        WordHolder nextVal = theQ.peek();
        while (nextVal != null &&
               nextVal.getIterator().startIndex < itr.wordsWalked) {
          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          if (i.startIndex == itr.startIndex) {
            // if a literal was created from a flip bit, OR it with other literals or literals from flip bits in the same
            // position
            if (ConciseSetUtils.isOneSequence(w)) {
              flipBitLiteral |= ConciseSetUtils.getLiteralFromOneSeqFlipBit(w);
            } else if (ConciseSetUtils.isLiteral(w)) {
              flipBitLiteral |= w;
            } else {
              flipBitLiteral |= ConciseSetUtils.getLiteralFromZeroSeqFlipBit(w);
            }
          }

          i.advanceTo(itr.wordsWalked);
          if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          }
          nextVal = theQ.peek();
        }

        // advance longest one literal forward and push result back to priority queue
        // if a flip bit is still needed, put it in the correct position
        int newWord = word & 0xC1FFFFFF;
        if (flipBitLiteral != ConciseSetUtils.ALL_ONES_LITERAL) {
          flipBitLiteral ^= ConciseSetUtils.ALL_ONES_LITERAL;
          int position = Integer.numberOfTrailingZeros(flipBitLiteral) + 1;
          newWord |= (position << 25);
        }
        addAndCompact(retVal, newWord);
        currIndex = itr.wordsWalked;

        if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        }
      } else if (ConciseSetUtils.isLiteral(word)) {
        // advance all other literals
        WordHolder nextVal = theQ.peek();
        while (nextVal != null &&
               nextVal.getIterator().startIndex == itr.startIndex) {

          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          // if we still have zero fills with flipped bits, OR them here
          if (ConciseSetUtils.isLiteral(w)) {
            word |= w;
          } else {
            int flipBitLiteral = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(w);
            if (flipBitLiteral != ConciseSetUtils.ALL_ZEROS_LITERAL) {
              word |= flipBitLiteral;
              i.advanceTo(itr.wordsWalked);
            }
          }

          if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          }

          nextVal = theQ.peek();
        }

        // advance the set with the current literal forward and push result back to priority queue
        addAndCompact(retVal, word);
        currIndex++;

        if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        }
      } else { // zero fills
        int flipBitLiteral;
        WordHolder nextVal = theQ.peek();

        while (nextVal != null &&
               nextVal.getIterator().startIndex == itr.startIndex) {
          // check if literal can be created flip bits of other zero sequences
          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          flipBitLiteral = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(w);
          if (flipBitLiteral != ConciseSetUtils.ALL_ZEROS_LITERAL) {
            wordsToAdd.add(new WordHolder(flipBitLiteral, i));
          } else if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          }
          nextVal = theQ.peek();
        }

        // check if a literal needs to be created from the flipped bits of this sequence
        flipBitLiteral = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(word);
        if (flipBitLiteral != ConciseSetUtils.ALL_ZEROS_LITERAL) {
          wordsToAdd.add(new WordHolder(flipBitLiteral, itr));
        } else if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        }
      }

      theQ.addAll(wordsToAdd);
    }

    if (retVal.isEmpty()) {
      return new ImmutableConciseSet();
    }
    return new ImmutableConciseSet(IntBuffer.wrap(retVal.toArray()));
  }

  public static ImmutableConciseSet doIntersection(Iterator<ImmutableConciseSet> sets)
  {
    IntList retVal = new IntList();

    // lhs = current word position, rhs = the iterator
    // Comparison is first by index, then zero fills > literals > one fills
    // zero fills are sorted by length (longer zero fills have priority)
    // similarily, shorter one fills have priority
    MinMaxPriorityQueue<WordHolder> theQ = MinMaxPriorityQueue.orderedBy(
        new Comparator<WordHolder>()
        {
          @Override
          public int compare(WordHolder h1, WordHolder h2)
          {
            int w1 = h1.getWord();
            int w2 = h2.getWord();
            int s1 = h1.getIterator().startIndex;
            int s2 = h2.getIterator().startIndex;

            if (s1 != s2) {
              return compareInts(s1, s2);
            }

            if (ConciseSetUtils.isZeroSequence(w1)) {
              if (ConciseSetUtils.isZeroSequence(w2)) {
                return -compareInts(ConciseSetUtils.getSequenceNumWords(w1), ConciseSetUtils.getSequenceNumWords(w2));
              }
              return -1;
            } else if (ConciseSetUtils.isLiteral(w1)) {
              if (ConciseSetUtils.isZeroSequence(w2)) {
                return 1;
              } else if (ConciseSetUtils.isLiteral(w2)) {
                return 0;
              }
              return -1;
            } else {
              if (!ConciseSetUtils.isOneSequence(w2)) {
                return 1;
              }
              return compareInts(ConciseSetUtils.getSequenceNumWords(w1), ConciseSetUtils.getSequenceNumWords(w2));
            }
          }
        }
    ).create();

    // populate priority queue
    while (sets.hasNext()) {
      ImmutableConciseSet set = sets.next();

      if (set == null || set.isEmpty()) {
        return new ImmutableConciseSet();
      }

      WordIterator itr = set.newWordIterator();
      theQ.add(new WordHolder(itr.next(), itr));
    }

    int currIndex = 0;
    int wordsWalkedAtSequenceEnd = Integer.MAX_VALUE;

    while (!theQ.isEmpty()) {
      // create a temp list to hold everything that will get pushed back into the priority queue after each run
      List<WordHolder> wordsToAdd = Lists.newArrayList();

      // grab the top element from the priority queue
      WordHolder curr = theQ.poll();
      int word = curr.getWord();
      WordIterator itr = curr.getIterator();

      // if a sequence has ended, we can break out because of Boolean logic
      if (itr.startIndex >= wordsWalkedAtSequenceEnd) {
        break;
      }

      // if the next word in the queue starts at a different point than where we ended off we need to create a one gap
      // to fill the space
      if (currIndex < itr.startIndex) {
        // number of 31 bit blocks that compromise the fill minus one
        addAndCompact(retVal, (ConciseSetUtils.SEQUENCE_BIT | (itr.startIndex - currIndex - 1)));
        currIndex = itr.startIndex;
      }

      if (ConciseSetUtils.isZeroSequence(word)) {
        // extract a literal from the flip bits of the zero sequence
        int flipBitLiteral = ConciseSetUtils.getLiteralFromZeroSeqFlipBit(word);

        // advance everything past the longest zero sequence
        WordHolder nextVal = theQ.peek();
        while (nextVal != null &&
               nextVal.getIterator().startIndex < itr.wordsWalked) {
          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          if (i.startIndex == itr.startIndex) {
            // if a literal was created from a flip bit, AND it with other literals or literals from flip bits in the same
            // position
            if (ConciseSetUtils.isZeroSequence(w)) {
              flipBitLiteral &= ConciseSetUtils.getLiteralFromZeroSeqFlipBit(w);
            } else if (ConciseSetUtils.isLiteral(w)) {
              flipBitLiteral &= w;
            } else {
              flipBitLiteral &= ConciseSetUtils.getLiteralFromOneSeqFlipBit(w);
            }
          }

          i.advanceTo(itr.wordsWalked);
          if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          } else {
            wordsWalkedAtSequenceEnd = Math.min(i.wordsWalked, wordsWalkedAtSequenceEnd);
          }
          nextVal = theQ.peek();
        }

        // advance longest zero literal forward and push result back to priority queue
        // if a flip bit is still needed, put it in the correct position
        int newWord = word & 0xC1FFFFFF;
        if (flipBitLiteral != ConciseSetUtils.ALL_ZEROS_LITERAL) {
          int position = Integer.numberOfTrailingZeros(flipBitLiteral) + 1;
          newWord = (word & 0xC1FFFFFF) | (position << 25);
        }
        addAndCompact(retVal, newWord);
        currIndex = itr.wordsWalked;

        if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        } else {
          wordsWalkedAtSequenceEnd = Math.min(itr.wordsWalked, wordsWalkedAtSequenceEnd);
        }
      } else if (ConciseSetUtils.isLiteral(word)) {
        // advance all other literals
        WordHolder nextVal = theQ.peek();
        while (nextVal != null &&
               nextVal.getIterator().startIndex == itr.startIndex) {

          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          // if we still have one fills with flipped bits, AND them here
          if (ConciseSetUtils.isLiteral(w)) {
            word &= w;
          } else {
            int flipBitLiteral = ConciseSetUtils.getLiteralFromOneSeqFlipBit(w);
            if (flipBitLiteral != ConciseSetUtils.ALL_ONES_LITERAL) {
              word &= flipBitLiteral;
              i.advanceTo(itr.wordsWalked);
            }
          }

          if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          } else {
            wordsWalkedAtSequenceEnd = Math.min(i.wordsWalked, wordsWalkedAtSequenceEnd);
          }

          nextVal = theQ.peek();
        }

        // advance the set with the current literal forward and push result back to priority queue
        addAndCompact(retVal, word);
        currIndex++;

        if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        } else {
          wordsWalkedAtSequenceEnd = Math.min(itr.wordsWalked, wordsWalkedAtSequenceEnd);
        }
      } else { // one fills
        int flipBitLiteral;
        WordHolder nextVal = theQ.peek();

        while (nextVal != null &&
               nextVal.getIterator().startIndex == itr.startIndex) {
          // check if literal can be created flip bits of other one sequences
          WordHolder entry = theQ.poll();
          int w = entry.getWord();
          WordIterator i = entry.getIterator();

          flipBitLiteral = ConciseSetUtils.getLiteralFromOneSeqFlipBit(w);
          if (flipBitLiteral != ConciseSetUtils.ALL_ONES_LITERAL) {
            wordsToAdd.add(new WordHolder(flipBitLiteral, i));
          } else if (i.hasNext()) {
            wordsToAdd.add(new WordHolder(i.next(), i));
          } else {
            wordsWalkedAtSequenceEnd = Math.min(i.wordsWalked, wordsWalkedAtSequenceEnd);
          }

          nextVal = theQ.peek();
        }

        // check if a literal needs to be created from the flipped bits of this sequence
        flipBitLiteral = ConciseSetUtils.getLiteralFromOneSeqFlipBit(word);
        if (flipBitLiteral != ConciseSetUtils.ALL_ONES_LITERAL) {
          wordsToAdd.add(new WordHolder(flipBitLiteral, itr));
        } else if (itr.hasNext()) {
          wordsToAdd.add(new WordHolder(itr.next(), itr));
        } else {
          wordsWalkedAtSequenceEnd = Math.min(itr.wordsWalked, wordsWalkedAtSequenceEnd);
        }
      }

      theQ.addAll(wordsToAdd);
    }

    // fill in any missing one sequences
    if (currIndex < wordsWalkedAtSequenceEnd) {
      addAndCompact(retVal, (ConciseSetUtils.SEQUENCE_BIT | (wordsWalkedAtSequenceEnd - currIndex - 1)));
    }

    if (retVal.isEmpty()) {
      return new ImmutableConciseSet();
    }
    return new ImmutableConciseSet(IntBuffer.wrap(retVal.toArray()));
  }

  public static ImmutableConciseSet doComplement(ImmutableConciseSet set)
  {
    if (set == null || set.isEmpty()) {
      return new ImmutableConciseSet();
    }

    IntList retVal = new IntList();
    WordIterator iter = set.newWordIterator();
    while (iter.hasNext()) {
      int word = iter.next();
      if (ConciseSetUtils.isLiteral(word)) {
        retVal.add(ConciseSetUtils.ALL_ZEROS_LITERAL | ~word);
      } else {
        retVal.add(ConciseSetUtils.SEQUENCE_BIT ^ word);
      }
    }
    // do not complement after the last element
    int lastWord = retVal.get(retVal.length() - 1);
    if (ConciseSetUtils.isLiteral(lastWord)) {
      lastWord = ConciseSetUtils.clearBitsAfterInLastWord(
          lastWord,
          ConciseSetUtils.maxLiteralLengthModulus(set.getLast())
      );
    }

    retVal.set(retVal.length() - 1, lastWord);

    trimZeros(retVal);

    if (retVal.isEmpty()) {
      return new ImmutableConciseSet();
    }
    return new ImmutableConciseSet(IntBuffer.wrap(retVal.toArray()));
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  private static void trimZeros(IntList set)
  {
    // loop over ALL_ZEROS_LITERAL words
    int w;
    int last = set.length() - 1;
    do {
      w = set.get(last);
      if (w == ConciseSetUtils.ALL_ZEROS_LITERAL) {
        set.set(last, 0);
        last--;
      } else if (ConciseSetUtils.isZeroSequence(w)) {
        if (ConciseSetUtils.isSequenceWithNoBits(w)) {
          set.set(last, 0);
          last--;
        } else {
          // convert the sequence in a 1-bit literal word
          set.set(last, ConciseSetUtils.getLiteral(w, false));
          return;
        }
      } else {
        // one sequence or literal
        return;
      }
      if (set.isEmpty() || last == -1) {
        return;
      }
    } while (true);
  }

  public byte[] toBytes()
  {
    if (words == null) {
      return new byte[]{};
    }
    ByteBuffer buf = ByteBuffer.allocate(words.capacity() * Ints.BYTES);
    buf.asIntBuffer().put(words.asReadOnlyBuffer());
    return buf.array();
  }

  public int getLastWordIndex()
  {
    return lastWordIndex;
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  private int calcSize()
  {
    int retVal = 0;
    for (int i = 0; i <= lastWordIndex; i++) {
      int w = words.get(i);
      if (ConciseSetUtils.isLiteral(w)) {
        retVal += ConciseSetUtils.getLiteralBitCount(w);
      } else {
        if (ConciseSetUtils.isZeroSequence(w)) {
          if (!ConciseSetUtils.isSequenceWithNoBits(w)) {
            retVal++;
          }
        } else {
          retVal += ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(w) + 1);
          if (!ConciseSetUtils.isSequenceWithNoBits(w)) {
            retVal--;
          }
        }
      }
    }

    return retVal;
  }

  public int size()
  {
    return size;
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  public int getLast()
  {
    if (isEmpty()) {
      return -1;
    }

    int last = 0;
    for (int i = 0; i <= lastWordIndex; i++) {
      int w = words.get(i);
      if (ConciseSetUtils.isLiteral(w)) {
        last += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        last += ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(w) + 1);
      }
    }

    int w = words.get(lastWordIndex);
    if (ConciseSetUtils.isLiteral(w)) {
      last -= Integer.numberOfLeadingZeros(ConciseSetUtils.getLiteralBits(w));
    } else {
      last--;
    }
    return last;
  }

  public boolean contains(final int integer)
  {
    if (isEmpty()) {
      return false;
    }
    final IntSet.IntIterator intIterator = iterator();
    intIterator.skipAllBefore(integer);
    return intIterator.hasNext() && intIterator.next() == integer;
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  public int get(int i)
  {
    if (i < 0) {
      throw new IndexOutOfBoundsException();
    }

    // initialize data
    int firstSetBitInWord = 0;
    int position = i;
    int setBitsInCurrentWord = 0;
    for (int j = 0; j <= lastWordIndex; j++) {
      int w = words.get(j);
      if (ConciseSetUtils.isLiteral(w)) {
        // number of bits in the current word
        setBitsInCurrentWord = ConciseSetUtils.getLiteralBitCount(w);

        // check if the desired bit is in the current word
        if (position < setBitsInCurrentWord) {
          int currSetBitInWord = -1;
          for (; position >= 0; position--) {
            currSetBitInWord = Integer.numberOfTrailingZeros(w & (0xFFFFFFFF << (currSetBitInWord + 1)));
          }
          return firstSetBitInWord + currSetBitInWord;
        }

        // skip the 31-bit block
        firstSetBitInWord += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        // number of involved bits (31 * blocks)
        int sequenceLength = ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(w) + 1);

        // check the sequence type
        if (ConciseSetUtils.isOneSequence(w)) {
          if (ConciseSetUtils.isSequenceWithNoBits(w)) {
            setBitsInCurrentWord = sequenceLength;
            if (position < setBitsInCurrentWord) {
              return firstSetBitInWord + position;
            }
          } else {
            setBitsInCurrentWord = sequenceLength - 1;
            if (position < setBitsInCurrentWord)
            // check whether the desired set bit is after the
            // flipped bit (or after the first block)
            {
              return firstSetBitInWord + position + (position < ConciseSetUtils.getFlippedBit(w) ? 0 : 1);
            }
          }
        } else {
          if (ConciseSetUtils.isSequenceWithNoBits(w)) {
            setBitsInCurrentWord = 0;
          } else {
            setBitsInCurrentWord = 1;
            if (position == 0) {
              return firstSetBitInWord + ConciseSetUtils.getFlippedBit(w);
            }
          }
        }

        // skip the 31-bit blocks
        firstSetBitInWord += sequenceLength;
      }

      // update the number of found set bits
      position -= setBitsInCurrentWord;
    }

    throw new IndexOutOfBoundsException(Integer.toString(i));
  }

  public int compareTo(ImmutableConciseSet other)
  {
    return words.asReadOnlyBuffer().compareTo(other.words.asReadOnlyBuffer());
  }

  private boolean isEmpty()
  {
    return words == null || words.limit() == 0;
  }

  @Override
  // Based on the AbstractIntSet implementation by Alessandro Colantonio
  public String toString()
  {
    IntSet.IntIterator itr = iterator();
    if (!itr.hasNext()) {
      return "[]";
    }

    StringBuilder sb = new StringBuilder();
    sb.append('[');
    for (; ; ) {
      sb.append(itr.next());
      if (!itr.hasNext()) {
        return sb.append(']').toString();
      }
      sb.append(", ");
    }
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  public IntSet.IntIterator iterator()
  {
    if (isEmpty()) {
      return new IntSet.IntIterator()
      {
        @Override
        public void skipAllBefore(int element) {/*empty*/}

        @Override
        public boolean hasNext() {return false;}

        @Override
        public int next() {throw new NoSuchElementException();}

        @Override
        public void remove() {throw new UnsupportedOperationException();}

        @Override
        public IntSet.IntIterator clone() {throw new UnsupportedOperationException();}
      };
    }
    return new BitIterator();
  }

  public WordIterator newWordIterator()
  {
    return new WordIterator();
  }

  private static class WordHolder
  {
    private final int word;
    private final WordIterator iterator;

    public WordHolder(
        int word,
        WordIterator iterator
    )
    {
      this.word = word;
      this.iterator = iterator;
    }

    public int getWord()
    {
      return word;
    }

    public WordIterator getIterator()
    {
      return iterator;
    }
  }

  // Based on the ConciseSet implementation by Alessandro Colantonio
  private class BitIterator implements IntSet.IntIterator
  {
    final ConciseSetUtils.LiteralAndZeroFillExpander litExp;
    final ConciseSetUtils.OneFillExpander oneExp;

    ConciseSetUtils.WordExpander exp;
    int nextIndex = 0;
    int nextOffset = 0;

    private BitIterator()
    {
      litExp = ConciseSetUtils.newLiteralAndZeroFillExpander();
      oneExp = ConciseSetUtils.newOneFillExpander();

      nextWord();
    }

    private BitIterator(
        ConciseSetUtils.LiteralAndZeroFillExpander litExp,
        ConciseSetUtils.OneFillExpander oneExp,
        ConciseSetUtils.WordExpander exp,
        int nextIndex,
        int nextOffset
    )
    {
      this.litExp = litExp;
      this.oneExp = oneExp;
      this.exp = exp;
      this.nextIndex = nextIndex;
      this.nextOffset = nextOffset;
    }

    @Override
    public boolean hasNext()
    {
      while (!exp.hasNext()) {
        if (nextIndex > lastWordIndex) {
          return false;
        }
        nextWord();
      }
      return true;
    }

    @Override
    public int next()
    {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return exp.next();
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    @Override
    public void skipAllBefore(int element)
    {
      while (true) {
        exp.skipAllBefore(element);
        if (exp.hasNext() || nextIndex > lastWordIndex) {
          return;
        }
        nextWord();
      }
    }

    @Override
    public IntSet.IntIterator clone()
    {
      return new BitIterator(
          (ConciseSetUtils.LiteralAndZeroFillExpander) litExp.clone(),
          (ConciseSetUtils.OneFillExpander) oneExp.clone(),
          exp.clone(),
          nextIndex,
          nextOffset
      );
    }

    private void nextWord()
    {
      final int word = words.get(nextIndex++);
      exp = ConciseSetUtils.isOneSequence(word) ? oneExp : litExp;
      exp.reset(nextOffset, word, true);

      // prepare next offset
      if (ConciseSetUtils.isLiteral(word)) {
        nextOffset += ConciseSetUtils.MAX_LITERAL_LENGTH;
      } else {
        nextOffset += ConciseSetUtils.maxLiteralLengthMultiplication(ConciseSetUtils.getSequenceCount(word) + 1);
      }
    }
  }

  public class WordIterator implements Iterator
  {
    private int startIndex;
    private int wordsWalked;
    private int currWord;
    private int nextWord;
    private int currRow;

    private volatile boolean hasNextWord = false;

    WordIterator()
    {
      startIndex = -1;
      wordsWalked = 0;
      currRow = -1;
    }

    public void advanceTo(int endCount)
    {
      while (hasNext() && wordsWalked < endCount) {
        next();
      }
      if (wordsWalked <= endCount) {
        return;
      }

      nextWord = (currWord & 0xC1000000) | (wordsWalked - endCount - 1);
      startIndex = endCount;
      hasNextWord = true;
    }

    @Override
    public boolean hasNext()
    {
      if (isEmpty()) {
        return false;
      }
      if (hasNextWord) {
        return true;
      }
      return currRow < (words.capacity() - 1);
    }

    @Override
    public Integer next()
    {
      if (hasNextWord) {
        currWord = nextWord;
        hasNextWord = false;
        return new Integer(currWord);
      }

      currWord = words.get(++currRow);
      if (ConciseSetUtils.isLiteral(currWord)) {
        startIndex = wordsWalked++;
      } else {
        startIndex = wordsWalked;
        wordsWalked += ConciseSetUtils.getSequenceNumWords(currWord);
      }

      return new Integer(currWord);
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }
  }
}
