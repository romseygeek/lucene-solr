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


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.search.BooleanClause.Occur;

import static org.apache.lucene.queries.spans.SpanTestUtil.spanFirstQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearOrderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNearUnorderedQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanNotQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanOrQuery;
import static org.apache.lucene.queries.spans.SpanTestUtil.spanTermQuery;


/**
 * TestExplanations subclass focusing on span queries
 */
public class TestSpanExplanations extends BaseExplanationTestCase {

  /** MACRO for SpanTermQuery */
  public SpanQuery st(String s) {
    return spanTermQuery(FIELD, s);
  }

  /** MACRO for SpanNotQuery */
  public SpanQuery snot(SpanQuery i, SpanQuery e) {
    return spanNotQuery(i, e);
  }

  /** MACRO for SpanOrQuery containing two SpanTerm queries */
  public SpanQuery sor(String s, String e) {
    return spanOrQuery(FIELD, s, e);
  }

  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery e) {
    return spanOrQuery(s, e);
  }

  /** MACRO for SpanOrQuery containing three SpanTerm queries */
  public SpanQuery sor(String s, String m, String e) {
    return spanOrQuery(FIELD, s, m, e);
  }
  /** MACRO for SpanOrQuery containing two SpanQueries */
  public SpanQuery sor(SpanQuery s, SpanQuery m, SpanQuery e) {
    return spanOrQuery(s, m, e);
  }

  /** MACRO for SpanNearQuery containing two SpanTerm queries */
  public SpanQuery snear(String s, String e, int slop, boolean inOrder) {
    return snear(st(s), st(e), slop, inOrder);
  }

  /** MACRO for SpanNearQuery containing two SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, e);
    } else {
      return spanNearUnorderedQuery(slop, s, e);
    }
  }


  /** MACRO for SpanNearQuery containing three SpanTerm queries */
  public SpanQuery snear(String s, String m, String e,
                         int slop, boolean inOrder) {
    return snear(st(s), st(m), st(e), slop, inOrder);
  }
  /** MACRO for SpanNearQuery containing three SpanQueries */
  public SpanQuery snear(SpanQuery s, SpanQuery m, SpanQuery e, int slop, boolean inOrder) {
    if (inOrder) {
      return spanNearOrderedQuery(slop, s, m, e);
    } else {
      return spanNearUnorderedQuery(slop, s, m, e);
    }
  }

  /** MACRO for SpanFirst(SpanTermQuery) */
  public SpanQuery sf(String s, int b) {
    return spanFirstQuery(st(s), b);
  }

  /* simple SpanTermQueries */
  
  public void testST1() throws Exception {
    SpanQuery q = st("w1");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testST2() throws Exception {
    SpanQuery q = st("w1");
    qtest(new BoostQuery(q, 1000), new int[] {0,1,2,3});
  }
  public void testST4() throws Exception {
    SpanQuery q = st("xx");
    qtest(q, new int[] {2,3});
  }
  public void testST5() throws Exception {
    SpanQuery q = st("xx");
    qtest(new BoostQuery(q, 1000), new int[] {2,3});
  }

  /* some SpanFirstQueries */
  
  public void testSF1() throws Exception {
    SpanQuery q = sf(("w1"),1);
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSF2() throws Exception {
    SpanQuery q = sf(("w1"),1);
    qtest(new BoostQuery(q, 1000), new int[] {0,1,2,3});
  }
  public void testSF4() throws Exception {
    SpanQuery q = sf(("xx"),2);
    qtest(q, new int[] {2});
  }
  public void testSF5() throws Exception {
    SpanQuery q = sf(("yy"),2);
    qtest(q, new int[] { });
  }
  public void testSF6() throws Exception {
    SpanQuery q = sf(("yy"),4);
    qtest(new BoostQuery(q, 1000), new int[] {2});
  }
  
  /* some SpanOrQueries */

  public void testSO1() throws Exception {
    SpanQuery q = sor("w1","QQ");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSO2() throws Exception {
    SpanQuery q = sor("w1","w3","zz");
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSO3() throws Exception {
    SpanQuery q = sor("w5","QQ","yy");
    qtest(q, new int[] {0,2,3});
  }
  public void testSO4() throws Exception {
    SpanQuery q = sor("w5","QQ","yy");
    qtest(q, new int[] {0,2,3});
  }

  
  
  /* some SpanNearQueries */
  
  public void testSNear1() throws Exception {
    SpanQuery q = snear("w1","QQ",100,true);
    qtest(q, new int[] {});
  }
  public void testSNear2() throws Exception {
    SpanQuery q = snear("w1","xx",100,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear3() throws Exception {
    SpanQuery q = snear("w1","xx",0,true);
    qtest(q, new int[] {2});
  }
  public void testSNear4() throws Exception {
    SpanQuery q = snear("w1","xx",1,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear5() throws Exception {
    SpanQuery q = snear("xx","w1",0,false);
    qtest(q, new int[] {2});
  }

  public void testSNear6() throws Exception {
    SpanQuery q = snear("w1","w2","QQ",100,true);
    qtest(q, new int[] {});
  }
  public void testSNear7() throws Exception {
    SpanQuery q = snear("w1","xx","w2",100,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear8() throws Exception {
    SpanQuery q = snear("w1","xx","w2",0,true);
    qtest(q, new int[] {2});
  }
  public void testSNear9() throws Exception {
    SpanQuery q = snear("w1","xx","w2",1,true);
    qtest(q, new int[] {2,3});
  }
  public void testSNear10() throws Exception {
    SpanQuery q = snear("xx","w1","w2",0,false);
    qtest(q, new int[] {2});
  }
  public void testSNear11() throws Exception {
    SpanQuery q = snear("w1","w2","w3",1,true);
    qtest(q, new int[] {0,1});
  }

  
  /* some SpanNotQueries */

  public void testSNot1() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot2() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    qtest(new BoostQuery(q, 1000), new int[] {0,1,2,3});
  }
  public void testSNot4() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    qtest(q, new int[] {0,1,2,3});
  }
  public void testSNot5() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    qtest(new BoostQuery(q, 1000), new int[] {0,1,2,3});
  }
  public void testSNot7() throws Exception {
    SpanQuery f = snear("w1","w3",10,true);
    SpanQuery q = snot(f, st("xx"));
    qtest(q, new int[] {0,1,3});
  }
  public void testSNot10() throws Exception {
    SpanQuery t = st("xx");
    SpanQuery q = snot(snear("w1","w3",10,true), t);
    qtest(q, new int[] {0,1,3});
  }

  public void test1() throws Exception {

    BooleanQuery.Builder q = new BooleanQuery.Builder();

    PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "w1", "w2");
    q.add(phraseQuery, Occur.MUST);
    q.add(snear(st("w2"),
        sor("w5","zz"),
        4, true),
        Occur.SHOULD);
    q.add(snear(sf("w3",2), st("w2"), st("w3"), 5, true),
        Occur.SHOULD);

    Query t = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST)
        .add(matchTheseItems(new int[] {1,3}), Occur.FILTER)
        .build();
    q.add(new BoostQuery(t, 1000), Occur.SHOULD);

    t = new ConstantScoreQuery(matchTheseItems(new int[] {0,2}));
    q.add(new BoostQuery(t, 30), Occur.SHOULD);

    List<Query> disjuncts = new ArrayList<>();
    disjuncts.add(snear(st("w2"),
        sor("w5","zz"),
        4, true));
    disjuncts.add(new TermQuery(new Term(FIELD, "QQ")));

    BooleanQuery.Builder xxYYZZ = new BooleanQuery.Builder();;
    xxYYZZ.add(new TermQuery(new Term(FIELD, "xx")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "zz")), Occur.MUST_NOT);

    disjuncts.add(xxYYZZ.build());

    BooleanQuery.Builder xxW1 = new BooleanQuery.Builder();;
    xxW1.add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST_NOT);
    xxW1.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST_NOT);

    disjuncts.add(xxW1.build());

    List<Query> disjuncts2 = new ArrayList<>();
    disjuncts2.add(new TermQuery(new Term(FIELD, "w1")));
    disjuncts2.add(new TermQuery(new Term(FIELD, "w2")));
    disjuncts2.add(new TermQuery(new Term(FIELD, "w3")));
    disjuncts.add(new DisjunctionMaxQuery(disjuncts2, 0.5f));

    q.add(new DisjunctionMaxQuery(disjuncts, 0.2f), Occur.SHOULD);

    BooleanQuery.Builder b = new BooleanQuery.Builder();;
    b.setMinimumNumberShouldMatch(2);
    b.add(snear("w1","w2",1,true), Occur.SHOULD);
    b.add(snear("w2","w3",1,true), Occur.SHOULD);
    b.add(snear("w1","w3",3,true), Occur.SHOULD);

    q.add(b.build(), Occur.SHOULD);

    qtest(q.build(), new int[] { 0,1,2 });
  }

  public void test2() throws Exception {

    BooleanQuery.Builder q = new BooleanQuery.Builder();

    PhraseQuery phraseQuery = new PhraseQuery(1, FIELD, "w1", "w2");
    q.add(phraseQuery, Occur.MUST);
    q.add(snear(st("w2"),
        sor("w5","zz"),
        4, true),
        Occur.SHOULD);
    q.add(snear(sf("w3",2), st("w2"), st("w3"), 5, true),
        Occur.SHOULD);

    Query t = new BooleanQuery.Builder()
        .add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST)
        .add(matchTheseItems(new int[] {1,3}), Occur.FILTER)
        .build();
    q.add(new BoostQuery(t, 1000), Occur.SHOULD);

    t = new ConstantScoreQuery(matchTheseItems(new int[] {0,2}));
    q.add(new BoostQuery(t, 20), Occur.SHOULD);

    List<Query> disjuncts = new ArrayList<>();
    disjuncts.add(snear(st("w2"),
        sor("w5","zz"),
        4, true));
    disjuncts.add(new TermQuery(new Term(FIELD, "QQ")));

    BooleanQuery.Builder xxYYZZ = new BooleanQuery.Builder();;
    xxYYZZ.add(new TermQuery(new Term(FIELD, "xx")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "yy")), Occur.SHOULD);
    xxYYZZ.add(new TermQuery(new Term(FIELD, "zz")), Occur.MUST_NOT);

    disjuncts.add(xxYYZZ.build());

    BooleanQuery.Builder xxW1 = new BooleanQuery.Builder();;
    xxW1.add(new TermQuery(new Term(FIELD, "xx")), Occur.MUST_NOT);
    xxW1.add(new TermQuery(new Term(FIELD, "w1")), Occur.MUST_NOT);

    disjuncts.add(xxW1.build());

    DisjunctionMaxQuery dm2 = new DisjunctionMaxQuery(
        Arrays.asList(
            new TermQuery(new Term(FIELD, "w1")),
            new TermQuery(new Term(FIELD, "w2")),
            new TermQuery(new Term(FIELD, "w3"))),
        0.5f);
    disjuncts.add(dm2);

    q.add(new DisjunctionMaxQuery(disjuncts, 0.2f), Occur.SHOULD);

    BooleanQuery.Builder builder = new BooleanQuery.Builder();;
    builder.setMinimumNumberShouldMatch(2);
    builder.add(snear("w1","w2",1,true), Occur.SHOULD);
    builder.add(snear("w2","w3",1,true), Occur.SHOULD);
    builder.add(snear("w1","w3",3,true), Occur.SHOULD);
    BooleanQuery b = builder.build();

    q.add(new BoostQuery(b, 0), Occur.SHOULD);

    qtest(q.build(), new int[] { 0,1,2 });
  }

  // :TODO: we really need more crazy complex cases.

  public void testST3() throws Exception {
    SpanQuery q = st("w1");
    bqtest(new SpanBoostQuery(q, 0), new int[] {0,1,2,3});
  }
  public void testST6() throws Exception {
    SpanQuery q = st("xx");
    qtest(new SpanBoostQuery(q, 0), new int[] {2,3});
  }

  public void testSF3() throws Exception {
    SpanQuery q = sf(("w1"),1);
    bqtest(new SpanBoostQuery(q, 0), new int[] {0,1,2,3});
  }
  public void testSF7() throws Exception {
    SpanQuery q = sf(("xx"),3);
    bqtest(new SpanBoostQuery(q, 0), new int[] {2,3});
  }

  public void testSNot3() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("QQ"));
    bqtest(new SpanBoostQuery(q, 0), new int[] {0,1,2,3});
  }
  public void testSNot6() throws Exception {
    SpanQuery q = snot(sf("w1",10),st("xx"));
    bqtest(new SpanBoostQuery(q, 0), new int[] {0,1,2,3});
  }

  public void testSNot8() throws Exception {
    // NOTE: using qtest not bqtest
    SpanQuery f = snear("w1","w3",10,true);
    f = new SpanBoostQuery(f, 0);
    SpanQuery q = snot(f, st("xx"));
    qtest(q, new int[] {0,1,3});
  }
  public void testSNot9() throws Exception {
    // NOTE: using qtest not bqtest
    SpanQuery t = st("xx");
    t = new SpanBoostQuery(t, 0);
    SpanQuery q = snot(snear("w1","w3",10,true), t);
    qtest(q, new int[] {0,1,3});
  }




}
