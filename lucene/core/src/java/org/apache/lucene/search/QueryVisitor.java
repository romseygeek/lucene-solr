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

import org.apache.lucene.index.Term;

/**
 * Interface to allow recursion through a query tree
 *
 * @see Query#visit(QueryVisitor)
 */
public interface QueryVisitor {

  /**
   * Called by leaf queries that match on a specific term
   *
   * @param term  the term the query will match on
   */
  void matchesTerm(Term term);

  /**
   * Called by leaf queries that do not match on terms
   * @param query the query
   */
  default void visitLeaf(Query query) {}

  /**
   * Pulls a visitor instance for visiting matching child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getMatchingVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching 'should-match' child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getShouldMatchVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching non-scoring child clauses of a query
   *
   * The default implementation returns {@code this}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getFilteringVisitor(Query parent) {
    return this;
  }

  /**
   * Pulls a visitor instance for visiting matching 'must-not' child clauses of a query
   *
   * The default implementation returns {@link #NO_OP}
   *
   * @param parent  the query visited
   */
  default QueryVisitor getNonMatchingVisitor(Query parent) {
    return NO_OP;
  }

  /**
   * A QueryVisitor implementation that collects no terms
   */
  QueryVisitor NO_OP = term -> { };

}
