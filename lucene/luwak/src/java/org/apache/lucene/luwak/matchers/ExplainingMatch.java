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

package org.apache.lucene.luwak.matchers;

import java.util.Objects;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.luwak.QueryMatch;

/**
 * A query match containing the score explanation of the match
 */
public class ExplainingMatch extends QueryMatch {

  private final Explanation explanation;

  ExplainingMatch(String queryId, Explanation explanation) {
    super(queryId);
    this.explanation = explanation;
  }

  /**
   * @return the Explanation
   */
  public Explanation getExplanation() {
    return explanation;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    if (!super.equals(o)) return false;
    ExplainingMatch that = (ExplainingMatch) o;
    return Objects.equals(explanation, that.explanation);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), explanation);
  }
}
