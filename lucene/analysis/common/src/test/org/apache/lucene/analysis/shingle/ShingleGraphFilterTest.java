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

package org.apache.lucene.analysis.shingle;

import java.io.IOException;
import java.io.StringReader;
import java.text.ParseException;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.MockAnalyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.custom.CustomAnalyzer;
import org.apache.lucene.analysis.synonym.SolrSynonymParser;
import org.apache.lucene.analysis.synonym.SynonymGraphFilter;
import org.apache.lucene.analysis.synonym.SynonymMap;
import org.apache.lucene.util.CharsRef;


public class ShingleGraphFilterTest extends BaseTokenStreamTestCase {

  public void testBiGramFilter() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("shinglegraph", "maxShingleSize", "2", "minShingleSize", "2")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please divide", "please", "divide this", "divide", "this sentence", "this", "sentence into", "sentence", "into shingles", "into", "shingles" },
          new int[] {     0,               0,        7,             7,        14,              14,     19,              19,         28,              28,     33 },
          new int[] {     13,              6,        18,            13,       27,              18,     32,              27,         41,              32,     41 },
          new String[] { "shingle",       "word",   "shingle",     "word",   "shingle",       "word", "shingle",       "word",     "shingle",       "word", "word" },
          new int[] {     1,               0,        1,             0,        1,               0,      1,               0,          1,               0,      1 },
          new int[] {     2,               1,        2,             1,        2,               1,      2,               1,          2,               1,      1 });
    }

  }

  public void testBiGramFilterWithAltSeparator() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("shinglegraph", "maxShingleSize", "2", "minShingleSize", "2", "outputUnigrams", "false",
            "tokenSeparator", "<SEP>")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please<SEP>divide", "divide<SEP>this", "this<SEP>sentence", "sentence<SEP>into", "into<SEP>shingles", "shingles" },
          new int[] {     0,               7,             14,              19,              28,              33 },
          new int[] {     13,              18,            27,              32,              41,              41 },
          new String[] { "shingle",       "shingle",     "shingle",       "shingle",       "shingle",       "word" },
          new int[] {     1,               1,             1,               1,               1,               1 },
          new int[] {     2,               2,             2,               2,               2,               1 });
    }


  }

  public void testBiGramNoUnigrams() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("shinglegraph", "maxShingleSize", "2", "minShingleSize", "2", "outputUnigrams", "false")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please divide", "divide this", "this sentence", "sentence into", "into shingles", "shingles" },
          new int[] {     0,               7,             14,              19,              28,              33 },
          new int[] {     13,              18,            27,              32,              41,              41 },
          new String[] { "shingle",       "shingle",     "shingle",       "shingle",       "shingle",       "word" },
          new int[] {     1,               1,             1,               1,               1,               1 },
          new int[] {     2,               2,             2,               2,               2,               1 });
    }

  }

  public void testTriGramFilter() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("shinglegraph", "maxShingleSize", "3", "minShingleSize", "2")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please divide this", "please divide", "please", "divide this sentence", "divide this", "divide",
                         "this sentence into", "this sentence", "this", "sentence into shingles", "sentence into", "sentence",
                         "into shingles", "into", "shingles" },
          new int[] {     0,                    0,               0,        7,                      7,             7,
                          14,                   14,              14,     19,                       19,              19,
                          28,              28,     33 },
          new int[] {     18,                   13,              6,        27,                     18,            13,
                          32,                   27,              18,     41,                       32,              27,
                          41,              32,     41 },
          new String[] { "shingle",             "shingle",       "word",   "shingle",              "shingle",     "word",
                         "shingle",             "shingle",       "word", "shingle",                "shingle",       "word",
                         "shingle",       "word", "word" },
          new int[] {     1,                     0,               0,        1,                      0,             0,
                          1,                     0,               0,      1,                        0,               0,
                          1,               0,      1 },
          new int[] {     3,                     2,               1,        3,                      2,             1,
                          3,                     2,               1,      3,                        2,               1,
                          2,               1,      1 });
    }

  }

  public void testTriGramNoUnigrams() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("shinglegraph", "maxShingleSize", "3", "minShingleSize", "2", "outputUnigrams", "false")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please divide this", "please divide", "divide this sentence", "divide this",
              "this sentence into", "this sentence", "sentence into shingles", "sentence into",
              "into shingles", "shingles" },
          new int[] {     0,                    0,               7,                      7,
              14,                   14,              19,                       19,
              28,              33 },
          new int[] {     18,                   13,              27,                     18,
              32,                   27,              41,                       32,
              41,              41 },
          new String[] { "shingle",             "shingle",       "shingle",              "shingle",
              "shingle",             "shingle",       "shingle",                "shingle",
              "shingle",       "word" },
          new int[] {     1,                     0,               1,                      0,
              1,                     0,               1,                        0,
              1,               1 },
          new int[] {     3,                     2,               3,                      2,
              3,                     2,               3,                        2,
              2,               1 });
    }

  }

  public void testWithStopwords() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("stop")
        .addTokenFilter("shinglegraph", "maxShingleSize", "3", "minShingleSize", "2")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "please divide this sentence into shingles")) {
      assertTokenStreamContents(ts,
          new String[] { "please divide _", "please divide", "please", "divide _ sentence", "divide _", "divide",
              "sentence _ shingles", "sentence _", "sentence", "shingles" },
          new int[] {     0,                 0,               0,        7,                   7,          7,
              19,                     19,           19,         33 },
          new int[] {     13,                13,              6,        27,                  13,         13,
              41,                     27,           27,         41 },
          new String[] { "shingle",          "shingle",       "word",   "shingle",           "shingle",  "word",
              "shingle",             "shingle",     "word",    "word" },
          new int[] {     1,                 0,               0,        1,                   0,          0,
              2,                     0,              0,        2 },
          new int[] {     3,                     2,           1,        3,                   2,          1,
              3,                     2,              1,        1 });


    }

  }

  public void testConsecutiveStopwords() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("stop")
        .addTokenFilter("shinglegraph", "maxShingleSize", "4", "minShingleSize", "4", "outputUnigrams", "false")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "a b c d a a b c")) {
      assertTokenStreamContents(ts,
          new String[] { "b c d _", "c d _ _", "d _ _ b", "b c", "c"},
          new int[] {    2,         4,         6,         12,      14 },
          new int[] {    7,         7,         13,        15,      15 },
          new int[] {    2,         1,         1,         3,       1 },
          new int[] {    4,         4,         4,         2,       1 },
          null);
    }

  }

  public void testTrailingStopwords() throws IOException {

    Analyzer analyzer = CustomAnalyzer.builder()
        .withTokenizer("whitespace")
        .addTokenFilter("stop")
        .addTokenFilter("shinglegraph", "maxShingleSize", "4", "minShingleSize", "4", "outputUnigrams", "false")
        .build();

    try (TokenStream ts = analyzer.tokenStream("field", "b c d a")) {
      assertTokenStreamContents(ts,
          new String[] { "b c d _", "c d _", "d _" },
          new int[] {    0,         2,       4 },
          new int[] {    5,         5,       5 },
          new int[] {    1,         1,         1 },
          new int[] {    4,         3,         2 },
          null);
    }

  }

  public void testIncomingGraphs() throws IOException {

    SynonymMap.Builder synonymBuilder = new SynonymMap.Builder();
    synonymBuilder.add(new CharsRef("a"), new CharsRef("b"), true);
    SynonymMap synonymMap = synonymBuilder.build();

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new WhitespaceTokenizer();
        TokenStream sink
            = new ShingleGraphFilter(new SynonymGraphFilter(source, synonymMap, true), 2, 2, false);
        return new TokenStreamComponents(source, sink);
      }
    };

    try (TokenStream ts = analyzer.tokenStream("field", "a c a d")) {
      assertTokenStreamContents(ts,
          new String[] { "b c", "a c", "c b", "c a", "b d", "a d", "d" },
          new int[] {    0,     0,     2,     2,     4,     4,     6 },
          new int[] {    3,     3,     5,     5,     7,     7,     7 },
          new int[] {    1,     0,     1,     0,     1,     0,     1 },
          new int[] {    2,     2,     2,     2,     2,     2,     1 },
          null);
    }

  }

  public void testShinglesSpanningGraphs() throws IOException {

    SynonymMap.Builder synonymBuilder = new SynonymMap.Builder();
    synonymBuilder.add(new CharsRef("a"), new CharsRef("b"), true);
    SynonymMap synonymMap = synonymBuilder.build();

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new WhitespaceTokenizer();
        TokenStream sink
            = new ShingleGraphFilter(new SynonymGraphFilter(source, synonymMap, true), 3, 3, false);
        return new TokenStreamComponents(source, sink);
      }
    };

    try (TokenStream ts = analyzer.tokenStream("field", "a c a d")) {
      assertTokenStreamContents(ts,
          new String[] { "b c b", "b c a", "a c b", "a c a", "c b d", "c a d", "b d", "a d", "d" },
          new int[] {    0,        0,      0,       0,       2,        2,       4,     4,     6 },
          new int[] {    5,        5,      5,       5,       7,        7,       7,     7,     7 },
          new int[] {    1,        0,      0,       0,       1,        0,       1,     0,     1 },
          new int[] {    3,        3,      3,       3,       3,        3,       2,     2,     1 },
          null);
    }

  }

  public void testMultiLengthPathShingles() throws IOException, ParseException {

    String testFile = "usa,united states,united states of america";
    Analyzer mockAnalyzer = new MockAnalyzer(random());
    SolrSynonymParser parser = new SolrSynonymParser(true, true, mockAnalyzer);

    parser.parse(new StringReader(testFile));
    mockAnalyzer.close();

    SynonymMap synonymMap = parser.build();

    Analyzer analyzer = new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String fieldName) {
        Tokenizer source = new WhitespaceTokenizer();
        TokenStream sink
            = new ShingleGraphFilter(new SynonymGraphFilter(source, synonymMap, true), 3, 3, false);
        return new TokenStreamComponents(source, sink);
      }
    };

    try (TokenStream ts = analyzer.tokenStream("field", "the usa is big")) {
      assertTokenStreamContents(ts,
          new String[]{ "the united states", "the united states", "the usa is", "united states is", "united states of",
              "usa is big", "states is big", "states of america", "of america is", "america is big", "is big", "big" },
          new int[]{    0,                   0,                   0,            4,                  4,
              4,            4,                   4,               4,               4,                8,        11 },
          new int[]{    7,                   7,                   10,           10,                 7,
              14,           14,                   7,              10,               14,               14,       14 },
          new int[]{    1,                   0,                   0,            1,                  0,
              0,            1,                   1,               1,               1,                1,        1 },
          new int[]{    6,                   4,                   7,            6,                  4,
              7,            6,                   3,               3,               3,                2,        1 },
          null);
    }

  }

}
