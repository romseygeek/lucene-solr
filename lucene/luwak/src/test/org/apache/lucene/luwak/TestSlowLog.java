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

package org.apache.lucene.luwak;

import java.io.IOException;

import org.apache.lucene.document.Document;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.core.IsNot.not;

public class TestSlowLog extends MonitorTestBase {

  public static Query slowQuery(long delay) {
    return new Query() {
      @Override
      public String toString(String s) {
        return "";
      }

      @Override
      public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) {
        try {
          Thread.sleep(delay);
        } catch (InterruptedException e) {
          throw new RuntimeException(e);
        }
        return new MatchAllDocsQuery().createWeight(searcher, scoreMode, boost);
      }

      @Override
      public void visit(QueryVisitor visitor) {
        visitor.visitLeaf(this);
      }

      @Override
      public boolean equals(Object o) {
        return false;
      }

      @Override
      public int hashCode() {
        return 0;
      }
    };
  }

  @Test
  public void testSlowLog() throws IOException {

    try (Monitor monitor = newMonitor()) {
      monitor.register(
          new MonitorQuery("1", slowQuery(250)),
          new MonitorQuery("2", new MatchAllDocsQuery()),
          new MonitorQuery("3", slowQuery(250)));

      Document doc1 = new Document();

      MatchingQueries<QueryMatch> matches = monitor.match(doc1, QueryMatch.SIMPLE_MATCHER);
      String slowlog = matches.getSlowLog().toString();
      assertThat(slowlog, containsString("1 ["));
      assertThat(slowlog, containsString("3 ["));
      assertThat(slowlog, not(containsString("2 [")));

      monitor.setSlowLogLimit(1);
      matches = monitor.match(doc1, QueryMatch.SIMPLE_MATCHER);
      slowlog = matches.getSlowLog().toString();
      assertThat(slowlog, containsString("1 ["));
      assertThat(slowlog, containsString("2 ["));
      assertThat(slowlog, containsString("3 ["));

      monitor.setSlowLogLimit(2000000000000L);
      assertFalse(monitor.match(doc1, QueryMatch.SIMPLE_MATCHER).getSlowLog().iterator().hasNext());
    }
  }
}
