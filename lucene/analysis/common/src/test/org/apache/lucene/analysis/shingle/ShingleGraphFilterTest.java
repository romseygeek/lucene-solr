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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.BaseTokenStreamTestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.custom.CustomAnalyzer;


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

}
