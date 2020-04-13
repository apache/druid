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

package org.apache.druid.collections.bitmap;

import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.testing.CollectionTestSuiteBuilder;
import com.google.common.collect.testing.SampleElements;
import com.google.common.collect.testing.TestCollectionGenerator;
import com.google.common.collect.testing.features.CollectionFeature;
import com.google.common.collect.testing.features.CollectionSize;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.roaringbitmap.IntIterator;

import java.util.AbstractCollection;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BitmapIterationTest extends TestCase
{
  public static Test suite()
  {
    List<BitmapFactory> factories = Arrays.asList(
        new BitSetBitmapFactory(),
        new ConciseBitmapFactory()
        // Roaring iteration fails because it doesn't throw NoSuchElementException on next() call when there are no more
        // elements. Instead, it either throws NullPointerException or returns some unspecified value. If bitmap
        // iterators are always used correctly in shouldn't be a problem, but if next() is occasionally called after
        // hasNext() returned false and it returns some unspecified value, and everything continues to work without
        // indication that there was a error, it would be a bug that is very hard to catch.
        //
        // This line should be uncommented when Druid updates RoaringBitmap dependency to a version which includes a fix
        // for https://github.com/RoaringBitmap/RoaringBitmap/issues/129, or when RoaringBitmap is included into Druid
        // as a module and the issue is fixed there.

        //new RoaringBitmapFactory()
    );

    TestSuite suite = new TestSuite();
    for (BitmapFactory factory : factories) {
      suite.addTest(suiteForFactory(factory));
    }
    return suite;
  }

  private static Test suiteForFactory(BitmapFactory factory)
  {
    return CollectionTestSuiteBuilder
        .using(new BitmapCollectionGenerator(factory))
        .named("bitmap iteration tests of " + factory)
        .withFeatures(CollectionFeature.KNOWN_ORDER)
        .withFeatures(CollectionFeature.REJECTS_DUPLICATES_AT_CREATION)
        .withFeatures(CollectionFeature.RESTRICTS_ELEMENTS)
        .withFeatures(CollectionSize.ANY)
        .createTestSuite();
  }

  private static class BitmapCollection extends AbstractCollection<Integer>
  {
    private final ImmutableBitmap bitmap;
    private final int size;

    private BitmapCollection(ImmutableBitmap bitmap, int size)
    {
      this.bitmap = bitmap;
      this.size = size;
    }

    @Override
    public UnmodifiableIterator<Integer> iterator()
    {
      final IntIterator iterator = bitmap.iterator();
      return new UnmodifiableIterator<Integer>()
      {
        @Override
        public boolean hasNext()
        {
          return iterator.hasNext();
        }

        @Override
        public Integer next()
        {
          return iterator.next();
        }
      };
    }

    @Override
    public int size()
    {
      return size;
    }
  }

  private static class BitmapCollectionGenerator implements TestCollectionGenerator<Integer>
  {
    private final BitmapFactory factory;

    private BitmapCollectionGenerator(BitmapFactory factory)
    {
      this.factory = factory;
    }

    @Override
    public SampleElements<Integer> samples()
    {
      return new SampleElements.Ints();
    }

    @Override
    public BitmapCollection create(Object... objects)
    {
      MutableBitmap mutableBitmap = factory.makeEmptyMutableBitmap();
      for (Object element : objects) {
        mutableBitmap.add(((Integer) element));
      }
      return new BitmapCollection(factory.makeImmutableBitmap(mutableBitmap), objects.length);
    }

    @Override
    public Integer[] createArray(int n)
    {
      return new Integer[n];
    }

    @Override
    public Iterable<Integer> order(List<Integer> list)
    {
      Collections.sort(list);
      return list;
    }
  }
}
