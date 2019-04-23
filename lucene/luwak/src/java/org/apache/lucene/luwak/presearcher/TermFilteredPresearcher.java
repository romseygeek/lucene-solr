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

package org.apache.lucene.luwak.presearcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiPredicate;

import org.apache.lucene.analysis.FilteringTokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.luwak.Presearcher;
import org.apache.lucene.luwak.queryanalysis.QueryAnalyzer;
import org.apache.lucene.luwak.queryanalysis.QueryTree;
import org.apache.lucene.luwak.queryanalysis.TermWeightor;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefIterator;

/**
 * Presearcher implementation that uses terms extracted from queries to index
 * them in the Monitor, and builds a BooleanQuery from InputDocuments to match
 * them.
 * <p>
 * This Presearcher uses a QueryTermExtractor to extract terms from queries.
 */
public class TermFilteredPresearcher extends Presearcher {

  /**
   * The default TermWeightor, weighting by token length
   */
  public static final TermWeightor DEFAULT_WEIGHTOR = TermWeightor.DEFAULT;

  static {
    BooleanQuery.setMaxClauseCount(Integer.MAX_VALUE);
  }

  protected final QueryAnalyzer extractor;
  protected final TermWeightor weightor;

  private final Set<String> filterFields;
  private final List<CustomQueryHandler> queryHandlers = new ArrayList<>();

  public static final String ANYTOKEN_FIELD = "__anytokenfield";

  public static final String ANYTOKEN = "__ANYTOKEN__";

  /**
   * Create a new TermFilteredPresearcher using a defined TermWeightor
   *
   * @param weightor   the TermWeightor
   */
  public TermFilteredPresearcher(TermWeightor weightor, List<CustomQueryHandler> customQueryHandlers, Set<String> filterFields) {
    this.extractor = new QueryAnalyzer(customQueryHandlers);
    this.filterFields = filterFields;
    this.queryHandlers.addAll(customQueryHandlers);
    this.weightor = weightor;
  }

  /**
   * Create a new TermFilteredPresearcher using the default term weighting
   */
  public TermFilteredPresearcher() {
    this(DEFAULT_WEIGHTOR, Collections.emptyList(), Collections.emptySet());
  }

  @Override
  public final Query buildQuery(LeafReader reader, BiPredicate<String, BytesRef> termAcceptor) {
    try {
      DocumentQueryBuilder queryBuilder = getQueryBuilder();
      for (FieldInfo field : reader.getFieldInfos()) {

        TokenStream ts = new TermsEnumTokenStream(reader.terms(field.name).iterator());
        for (CustomQueryHandler handler : queryHandlers) {
          ts = handler.wrapTermStream(field.name, ts);
        }

        ts = new FilteringTokenFilter(ts) {
          TermToBytesRefAttribute termAtt = addAttribute(TermToBytesRefAttribute.class);
          @Override
          protected boolean accept() {
            return filterFields.contains(field.name) == false && termAcceptor.test(field.name, termAtt.getBytesRef());
          }
        };

        TermToBytesRefAttribute termAtt = ts.addAttribute(TermToBytesRefAttribute.class);
        while (ts.incrementToken()) {
          queryBuilder.addTerm(field.name, BytesRef.deepCopyOf(termAtt.getBytesRef()));
        }
        ts.close();

      }
      Query presearcherQuery = queryBuilder.build();

      BooleanQuery.Builder bq = new BooleanQuery.Builder();
      bq.add(presearcherQuery, BooleanClause.Occur.SHOULD);
      bq.add(new TermQuery(new Term(ANYTOKEN_FIELD, ANYTOKEN)), BooleanClause.Occur.SHOULD);
      presearcherQuery = bq.build();
      if (filterFields.isEmpty() == false) {
        bq = new BooleanQuery.Builder();
        bq.add(presearcherQuery, BooleanClause.Occur.MUST);
        Query filterQuery = buildFilterFields(reader);
        if (filterQuery != null) {
          bq.add(filterQuery, BooleanClause.Occur.FILTER);
          presearcherQuery = bq.build();
        }
      }
      return presearcherQuery;
    } catch (IOException e) {
      // We're a MemoryIndex, so this shouldn't happen...
      throw new RuntimeException(e);
    }
  }

  private Query buildFilterFields(LeafReader reader) throws IOException {
    BooleanQuery.Builder builder = new BooleanQuery.Builder();
    for (String field : filterFields) {
      Query q = buildFilterClause(reader, field);
      if (q != null) {
        builder.add(q, BooleanClause.Occur.MUST);
      }
    }
    BooleanQuery bq = builder.build();
    if (bq.clauses().size() == 0) {
      return null;
    }
    return bq;
  }

  private Query buildFilterClause(LeafReader reader, String field) throws IOException {

    Terms terms = reader.terms(field);
    if (terms == null)
      return null;

    BooleanQuery.Builder bq = new BooleanQuery.Builder();

    int docsInBatch = reader.maxDoc();

    BytesRef term;
    TermsEnum te = terms.iterator();
    while ((term = te.next()) != null) {
      // we need to check that every document in the batch has the same field values, otherwise
      // this filtering will not work
      if (te.docFreq() != docsInBatch)
        throw new IllegalArgumentException("Some documents in this batch do not have a term value of "
            + field + ":" + Term.toString(term));
      bq.add(new TermQuery(new Term(field, BytesRef.deepCopyOf(term))), BooleanClause.Occur.SHOULD);
    }

    BooleanQuery built = bq.build();

    if (built.clauses().size() == 0)
      return null;

    return built;
  }

  protected interface DocumentQueryBuilder {

    /**
     * Add a term from this document
     */
    void addTerm(String field, BytesRef term) throws IOException;

    /**
     * @return the final Query
     */
    Query build();

  }

  protected DocumentQueryBuilder getQueryBuilder() {
    return new DocumentQueryBuilder() {

      Map<String, List<BytesRef>> terms = new HashMap<>();

      @Override
      public void addTerm(String field, BytesRef term) {
        List<BytesRef> t = terms.computeIfAbsent(field, f -> new ArrayList<>());
        t.add(term);
      }

      @Override
      public Query build() {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (String field : terms.keySet()) {
          builder.add(new TermInSetQuery(field, terms.get(field)), BooleanClause.Occur.SHOULD);
        }
        return builder.build();
      }
    };
  }

  static final FieldType QUERYFIELDTYPE;

  static {
    QUERYFIELDTYPE = new FieldType(TextField.TYPE_NOT_STORED);
    QUERYFIELDTYPE.setIndexOptions(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS);
    QUERYFIELDTYPE.freeze();
  }

  @Override
  public final Document indexQuery(Query query, Map<String, String> metadata) {
    QueryTree querytree = extractor.buildTree(query, weightor);
    Document doc = buildQueryDocument(querytree);
    for (String field : filterFields) {
      if (metadata != null && metadata.containsKey(field)) {
        doc.add(new TextField(field, metadata.get(field), Field.Store.YES));
      }
    }
    return doc;
  }

  protected Document buildQueryDocument(QueryTree querytree) {
    Map<String, BytesRefHash> fieldTerms = collectTerms(querytree);
    Document doc = new Document();
    for (Map.Entry<String, BytesRefHash> entry : fieldTerms.entrySet()) {
      doc.add(new Field(entry.getKey(),
          new TermsEnumTokenStream(new BytesRefHashIterator(entry.getValue())), QUERYFIELDTYPE));
    }
    return doc;
  }

  protected Map<String, BytesRefHash> collectTerms(QueryTree querytree) {
    Map<String, BytesRefHash> fieldTerms = new HashMap<>();
    querytree.collectTerms((field, term) -> {
      BytesRefHash tt = fieldTerms.computeIfAbsent(field, f -> new BytesRefHash());
      tt.add(term);
    });
    return fieldTerms;
  }

  protected class BytesRefHashIterator implements BytesRefIterator {

    final BytesRef scratch = new BytesRef();
    final BytesRefHash terms;
    final int[] sortedTerms;
    int upto = -1;


    BytesRefHashIterator(BytesRefHash terms) {
      this.terms = terms;
      this.sortedTerms = terms.sort();
    }

    @Override
    public BytesRef next() {
      if (upto >= sortedTerms.length)
        return null;
      upto++;
      if (sortedTerms[upto] == -1)
        return null;
      this.terms.get(sortedTerms[upto], scratch);
      return scratch;
    }
  }

}
