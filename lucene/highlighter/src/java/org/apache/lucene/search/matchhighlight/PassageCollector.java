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

package org.apache.lucene.search.matchhighlight;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.search.Matches;
import org.apache.lucene.search.MatchesIterator;

public class PassageCollector {

  private final Set<String> fields;
  private final int maxPassagesPerField;
  private final Supplier<PassageBuilder> passageSource;
  private final Map<String, PassageBuilder> builders = new HashMap<>();

  private Matches matches;
  private final Map<String, Iterator> iterators = new HashMap<>();

  private static class Iterator {
    final MatchesIterator mi;
    boolean exhausted = false;

    private Iterator(MatchesIterator mi) {
      this.mi = mi;
    }
  }

  public PassageCollector(Set<String> fields, int maxPassagesPerField, Supplier<PassageBuilder> passageSource) {
    this.fields = fields;
    this.maxPassagesPerField = maxPassagesPerField;
    this.passageSource = passageSource;
  }

  public void setMatches(Matches matches) {
    this.matches = matches;
  }

  public Document getHighlights() {
    Document document = new Document();
    for (Map.Entry<String, PassageBuilder> passages : builders.entrySet()) {
      for (String snippet : passages.getValue().getTopPassages(maxPassagesPerField)) {
        document.add(new TextField(passages.getKey(), snippet, Field.Store.YES));
      }
    }
    return document;
  }

  public Set<String> requiredFields() {
    return fields;
  }

  public void collectHighlights(String field, String text, int offset) throws IOException {

    Iterator mi = iterators.get(field);
    if (mi == null) {
      mi = new Iterator(matches.getMatches(field));
      if (mi.mi == null) {
        mi.exhausted = true;
      }
      iterators.put(field, mi);
      if (mi.exhausted == false) {
        assert mi.mi != null;
        if (mi.mi.next() == false) {
          mi.exhausted = true;
        }
      }
    }
    if (mi.exhausted) {
      return;
    }

    PassageBuilder passageBuilder = builders.computeIfAbsent(field, t -> passageSource.get());
    if (passageBuilder.build(text, mi.mi, offset) == false) {
      mi.exhausted = true;
    }

  }

}
