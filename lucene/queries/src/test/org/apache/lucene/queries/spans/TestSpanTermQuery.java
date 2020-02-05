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
package org.apache.lucene.queries.spans;


import java.io.IOException;
import java.util.Arrays;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.QueryUtils;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.LuceneTestCase;

/** Basic tests for SpanTermQuery */
public class TestSpanTermQuery extends LuceneTestCase {
  
  public void testHashcodeEquals() {
    SpanTermQuery q1 = new SpanTermQuery(new Term("field", "foo"));
    SpanTermQuery q2 = new SpanTermQuery(new Term("field", "bar"));
    QueryUtils.check(q1);
    QueryUtils.check(q2);
    QueryUtils.checkUnequal(q1, q2);
  }
  
  public void testNoPositions() throws IOException {
    Directory dir = newDirectory();
    RandomIndexWriter iw = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new StringField("foo", "bar", Field.Store.NO));
    iw.addDocument(doc);
    
    IndexReader ir = iw.getReader();
    iw.close();
    
    IndexSearcher is = new IndexSearcher(ir);
    SpanTermQuery query = new SpanTermQuery(new Term("foo", "bar"));
    IllegalStateException expected = expectThrows(IllegalStateException.class, () -> {
      is.search(query, 5);
    });
    assertTrue(expected.getMessage().contains("was indexed without position data"));

    ir.close();
    dir.close();
  }

  // LUCENE-4477 / LUCENE-4401:
  public void testBooleanSpanQuery() throws Exception {
    boolean failed = false;
    int hits = 0;
    Directory directory = newDirectory();
    Analyzer indexerAnalyzer = new MockAnalyzer(random());

    IndexWriterConfig config = new IndexWriterConfig(indexerAnalyzer);
    IndexWriter writer = new IndexWriter(directory, config);
    String FIELD = "content";
    Document d = new Document();
    d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
    writer.addDocument(d);
    writer.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(indexReader);

    BooleanQuery.Builder query = new BooleanQuery.Builder();
    SpanQuery sq1 = new SpanTermQuery(new Term(FIELD, "clockwork"));
    SpanQuery sq2 = new SpanTermQuery(new Term(FIELD, "clckwork"));
    query.add(sq1, BooleanClause.Occur.SHOULD);
    query.add(sq2, BooleanClause.Occur.SHOULD);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, Integer.MAX_VALUE);
    searcher.search(query.build(), collector);
    hits = collector.topDocs().scoreDocs.length;
    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs){
      System.out.println(scoreDoc.doc);
    }
    indexReader.close();
    assertEquals("Bug in boolean query composed of span queries", failed, false);
    assertEquals("Bug in boolean query composed of span queries", hits, 1);
    directory.close();
  }

  // LUCENE-4477 / LUCENE-4401:
  public void testDismaxSpanQuery() throws Exception {
    int hits = 0;
    Directory directory = newDirectory();
    Analyzer indexerAnalyzer = new MockAnalyzer(random());

    IndexWriterConfig config = new IndexWriterConfig(indexerAnalyzer);
    IndexWriter writer = new IndexWriter(directory, config);
    String FIELD = "content";
    Document d = new Document();
    d.add(new TextField(FIELD, "clockwork orange", Field.Store.YES));
    writer.addDocument(d);
    writer.close();

    IndexReader indexReader = DirectoryReader.open(directory);
    IndexSearcher searcher = newSearcher(indexReader);

    DisjunctionMaxQuery query = new DisjunctionMaxQuery(
        Arrays.asList(
            new SpanTermQuery(new Term(FIELD, "clockwork")),
            new SpanTermQuery(new Term(FIELD, "clckwork"))),
        1.0f);
    TopScoreDocCollector collector = TopScoreDocCollector.create(1000, Integer.MAX_VALUE);
    searcher.search(query, collector);
    hits = collector.topDocs().scoreDocs.length;
    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs){
      System.out.println(scoreDoc.doc);
    }
    indexReader.close();
    assertEquals(hits, 1);
    directory.close();
  }
}
