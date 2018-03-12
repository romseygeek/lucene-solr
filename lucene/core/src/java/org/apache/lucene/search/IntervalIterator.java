/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.search;

import java.io.IOException;

/**
 * A {@link DocIdSetIterator} than also allows iteration over matching
 * intervals in a document.
 *
 * Once the iterator is positioned on a document by calling {@link #advance(int)}
 * or {@link #nextDoc()}, intervals may be retrieved by calling {@link #nextInterval()}
 * until {@link #NO_MORE_INTERVALS} is returned.
 *
 * The limits of the current interval are returned by {@link #start()} and {@link #end()}.
 * When the iterator has been moved to a new document, but before {@link #nextInterval()}
 * has been called, both these methods return {@code -1}.
 *
 * Note that it is possible for a document to return {@link #NO_MORE_INTERVALS}
 * on the first call to {@link #nextInterval()}
 */
public abstract class IntervalIterator extends DocIdSetIterator {

  protected final DocIdSetIterator approximation;

  protected IntervalIterator(DocIdSetIterator approximation) {
    this.approximation = approximation;
  }

  /**
   * When returned from {@link #nextInterval()}, indicates that there are no more
   * matching intervals on the current document
   */
  public static final int NO_MORE_INTERVALS = Integer.MAX_VALUE;

  @Override
  public final int docID() {
    return approximation.docID();
  }

  @Override
  public final int nextDoc() throws IOException {
    int doc = approximation.nextDoc();
    reset();
    return doc;
  }

  @Override
  public final int advance(int target) throws IOException {
    int doc = approximation.advance(target);
    reset();
    return doc;
  }

  @Override
  public final long cost() {
    return approximation.cost();
  }

  /**
   * The start of the current interval
   *
   * Returns -1 if {@link #nextInterval()} has not yet been called
   */
  public abstract int start();

  /**
   * The end of the current interval
   *
   * Returns -1 if {@link #nextInterval()} has not yet been called
   */
  public abstract int end();

  /**
   * Advance the iterator to the next interval
   *
   * @return the starting interval of the next interval, or {@link IntervalIterator#NO_MORE_INTERVALS} if
   *         there are no more intervals on the current document
   */
  public abstract int nextInterval() throws IOException;

  /**
   * An indication of the average cost of iterating over all intervals in a document
   *
   * @see TwoPhaseIterator#matchCost()
   */
  public abstract float matchCost();

  /**
   * Called when the underlying iterator has been advanced.
   */
  protected abstract void reset() throws IOException;

  @Override
  public String toString() {
    return approximation.docID() + ":[" + start() + "->" + end() + "]";
  }

}
