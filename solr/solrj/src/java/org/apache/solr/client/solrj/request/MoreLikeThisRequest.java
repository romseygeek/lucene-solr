package org.apache.solr.client.solrj.request;

/*
 *   Copyright (c) 2016 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.response.MoreLikeThisResponse;
import org.apache.solr.common.params.CommonParams;
import org.apache.solr.common.params.ModifiableSolrParams;
import org.apache.solr.common.params.MoreLikeThisParams;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.ContentStream;
import org.apache.solr.common.util.ContentStreamBase;

public class MoreLikeThisRequest extends SolrRequest<MoreLikeThisResponse> {

  public static MoreLikeThisRequest byQuery(String query) {
    return new MoreLikeThisRequest(query, null);
  }

  public static MoreLikeThisRequest byContents(String contents) {
    return new MoreLikeThisRequest(null, contents);
  }

  // required params
  private final String query;
  private final String content;

  // optional params
  private String similarityFields = null;
  private Integer minTermFreq = null;
  private Integer minDocFreq = null;

  private MoreLikeThisRequest(String query, String content) {
    super(METHOD.GET, "/mlt");
    this.query = query;
    this.content = content;
  }

  @Override
  public SolrParams getParams() {
    ModifiableSolrParams params = new ModifiableSolrParams();
    if (query != null)
      params.set(CommonParams.Q, query);
    if (similarityFields != null)
      params.set(MoreLikeThisParams.SIMILARITY_FIELDS, similarityFields);
    if (minTermFreq != null)
      params.set(MoreLikeThisParams.MIN_TERM_FREQ, minTermFreq);
    if (minDocFreq != null)
      params.set(MoreLikeThisParams.MIN_DOC_FREQ, minDocFreq);
    return params;
  }

  @Override
  public Collection<ContentStream> getContentStreams() throws IOException {
    ContentStream stream = content == null ? null : new ContentStreamBase.StringStream(content);
    return content == null ? null : Collections.singleton(stream);
  }

  @Override
  protected MoreLikeThisResponse createResponse(SolrClient client) {
    return new MoreLikeThisResponse();
  }

  public MoreLikeThisRequest setSimilarityFields(String fields) {
    this.similarityFields = fields;
    return this;
  }

  public MoreLikeThisRequest setMinTermFreq(int minTermFreq) {
    this.minTermFreq = minTermFreq;
    return this;
  }

  public MoreLikeThisRequest setMinDocFreq(int minDocFreq) {
    this.minDocFreq = minDocFreq;
    return this;
  }
}
