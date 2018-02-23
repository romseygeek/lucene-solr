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
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.util.PriorityQueue;

public final class Intervals {

  public static final int NO_MORE_INTERVALS = Integer.MAX_VALUE;

  public static Query orderedQuery(String field, int width, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.OrderedNearFunction(0, width));
  }

  public static Query orderedQuery(String field, int minWidth, int maxWidth, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.OrderedNearFunction(minWidth, maxWidth));
  }

  public static Query orderedQuery(String field, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), IntervalFunction.ORDERED);
  }

  public static Query unorderedQuery(String field, int width, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.UnorderedNearFunction(0, width));
  }

  public static Query unorderedQuery(String field, int minWidth, int maxWidth, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), new IntervalFunction.UnorderedNearFunction(minWidth, maxWidth));
  }

  public static Query unorderedQuery(String field, Query... subQueries) {
    return new IntervalQuery(field, Arrays.asList(subQueries), IntervalFunction.UNORDERED);
  }

  public static Query nonOverlappingQuery(String field, Query minuend, Query subtrahend) {
    return new ContainingIntervalQuery(field, minuend, subtrahend, IntervalDifferenceFunction.NON_OVERLAPPING);
  }

  public static Query notWithinQuery(String field, Query minuend, int positions, Query subtrahend) {
    return new ContainingIntervalQuery(field, minuend, subtrahend, new IntervalDifferenceFunction.NotWithinFunction(positions));
  }


}
