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
import java.util.LinkedList;
import java.util.List;

import org.apache.lucene.analysis.TokenFilter;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionLengthAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.util.AttributeSource;

public final class ShingleGraphFilter extends TokenFilter {

  private final List<Token> tokenPool = new LinkedList<>();

  private final int minShingleSize;
  private final int maxShingleSize;
  private final boolean emitUnigrams;
  private final String tokenSeparator;
  private final Token GAP_TOKEN = new Token(new AttributeSource());
  private final Token END_TOKEN = new Token(new AttributeSource());

  private final PositionLengthAttribute lenAtt = addAttribute(PositionLengthAttribute.class);
  private final PositionIncrementAttribute incAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private Token[] currentShingleTokens;
  private int shingleSize;
  private boolean unigramDone;

  public ShingleGraphFilter(TokenStream input, int minShingleSize, int maxShingleSize, boolean emitUnigrams) {
    this(input, minShingleSize, maxShingleSize, emitUnigrams, " ", "_");
  }

  public ShingleGraphFilter(TokenStream input, int minShingleSize, int maxShingleSize, boolean emitUnigrams, String tokenSeparator, String fillerToken) {
    super(input);
    this.minShingleSize = minShingleSize;
    this.maxShingleSize = maxShingleSize;
    this.emitUnigrams = emitUnigrams;
    this.tokenSeparator = tokenSeparator;

    this.GAP_TOKEN.termAtt.setEmpty().append(fillerToken);

    this.currentShingleTokens = new Token[maxShingleSize];
  }

  @Override
  public boolean incrementToken() throws IOException {
    int posInc = 0;
    if (nextShingle() == false) {
      Token nextRoot = nextTokenInStream(currentShingleTokens[0]);
      if (nextRoot == END_TOKEN)
        return false;
      recycleToken(currentShingleTokens[0]);
      resetShingleRoot(nextRoot);
      posInc = currentShingleTokens[0].posInc();
    }
    clearAttributes();
    lenAtt.setPositionLength(shingleLength());
    incAtt.setPositionIncrement(posInc);
    offsetAtt.setOffset(currentShingleTokens[0].startOffset(), lastTokenInShingle().endOffset());
    termAtt.setEmpty();
    termAtt.append(currentShingleTokens[0].term());
    typeAtt.setType(shingleSize > 1 ? "shingle" : currentShingleTokens[0].type());
    for (int i = 1; i < shingleSize; i++) {
      termAtt.append(tokenSeparator).append(currentShingleTokens[i].term());
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.currentShingleTokens[0] = null;
  }

  private Token lastTokenInShingle() {
    int lastTokenIndex = shingleSize - 1;
    while (currentShingleTokens[lastTokenIndex] == GAP_TOKEN) {
      lastTokenIndex--;
    }
    return currentShingleTokens[lastTokenIndex];
  }

  private void resetShingleRoot(Token token) throws IOException {
    this.currentShingleTokens[0] = token;
    this.shingleSize = maxShingleSize;
    this.unigramDone = !emitUnigrams;
    token: for (int i = 1; i < maxShingleSize; i++) {
      Token current = nextTokenInGraph(this.currentShingleTokens[i - 1]);
      if (current == END_TOKEN) {
        this.shingleSize = i + END_TOKEN.posInc();
        if (this.shingleSize > 1) {
          // fill in any trailing gaps
          for (int j = 1; j < shingleSize; j++) {
            this.currentShingleTokens[i] = GAP_TOKEN;
            i++;
            if (i >= maxShingleSize) {
              break token;
            }
          }
        }
        return;
      }
      if (current.posInc() > 1) {
        // insert gaps into the shingle list
        for (int j = 1; j < current.posInc(); j++) {
          this.currentShingleTokens[i] = GAP_TOKEN;
          i++;
          if (i >= maxShingleSize) {
            break token;
          }
        }
      }
      this.currentShingleTokens[i] = current;
    }
  }

  private boolean nextShingle() throws IOException {
    if (currentShingleTokens[0] == null)
      return false;
    if (shingleSize <= minShingleSize) {
      if (advanceStack()) {
        return true;
      }
      if (unigramDone || shingleSize == 1) {
        return false;
      }
      unigramDone = true;
      this.shingleSize = 1;
      return true;
    }
    shingleSize--;
    return true;
  }

  private int shingleLength() {
    int len = 0;
    for (int i = 0; i < shingleSize; i++) {
      len += currentShingleTokens[i].length();
    }
    return len;
  }

  // check if the next token in the tokenstream is at the same position as this one
  private boolean lastInStack(Token token) throws IOException {
    Token next = nextTokenInStream(token);
    return next == END_TOKEN || next.posInc() != 0;
  }
  
  private boolean advanceStack() throws IOException {
    for (int i = maxShingleSize - 1; i >= 1; i--) {
      if (lastInStack(currentShingleTokens[i]) == false) {
        currentShingleTokens[i] = nextTokenInStream(currentShingleTokens[i]);
        for (int j = i + 1; j < maxShingleSize; j++) {
          currentShingleTokens[j] = nextTokenInGraph(currentShingleTokens[j - 1]);
        }
        return true;
      }
    }
    return false;
  }

  private Token newToken() {
    Token token = tokenPool.size() == 0 ? new Token(this.cloneAttributes()) : tokenPool.remove(0);
    token.reset(this);
    return token;
  }

  private void recycleToken(Token token) {
    if (token == null)
      return;
    token.nextToken = null;
    tokenPool.add(token);
  }

  // for testing
  int instantiatedTokenCount() {
    int tokenCount = tokenPool.size() + 1;
    if (currentShingleTokens[0] == END_TOKEN || currentShingleTokens[0] == null)
      return tokenCount;
    for (Token t = currentShingleTokens[0]; t != END_TOKEN && t != null; t = t.nextToken) {
      tokenCount++;
    }
    return tokenCount;
  }

  private Token nextTokenInGraph(Token token) throws IOException {
    int length = token.length();
    while (length > 0) {
      token = nextTokenInStream(token);
      if (token == END_TOKEN)
        return END_TOKEN;
      length -= token.posInc();
    }
    return token;
  }

  private Token nextTokenInStream(Token token) throws IOException {
    if (token == null) {  // first call
      if (input.incrementToken() == false) {
        input.end();
        // check for gaps at the end of the tokenstream
        END_TOKEN.posIncAtt.setPositionIncrement(this.incAtt.getPositionIncrement());
        END_TOKEN.offsetAtt.setOffset(END_TOKEN.offsetAtt.startOffset(), this.offsetAtt.endOffset());
        return END_TOKEN;
      }
      return newToken();
    }
    if (token.nextToken == null) {  // end of cache, advance the underlying tokenstream
      if (input.incrementToken() == false) {
        input.end();
        // check for gaps at the end of the tokenstream
        END_TOKEN.posIncAtt.setPositionIncrement(this.incAtt.getPositionIncrement());
        END_TOKEN.offsetAtt.setOffset(END_TOKEN.offsetAtt.startOffset(), this.offsetAtt.endOffset());
        token.nextToken = END_TOKEN;
      }
      else {
        token.nextToken = newToken();
      }
    }
    return token.nextToken;
  }

  private static class Token {
    final AttributeSource attSource;
    final PositionLengthAttribute posLenAtt;
    final PositionIncrementAttribute posIncAtt;
    final CharTermAttribute termAtt;
    final OffsetAttribute offsetAtt;
    final TypeAttribute typeAtt;

    Token nextToken;

    Token(AttributeSource attSource) {
      this.attSource = attSource;
      this.posIncAtt = attSource.addAttribute(PositionIncrementAttribute.class);
      this.posLenAtt = attSource.addAttribute(PositionLengthAttribute.class);
      this.termAtt = attSource.addAttribute(CharTermAttribute.class);
      this.offsetAtt = attSource.addAttribute(OffsetAttribute.class);
      this.typeAtt = attSource.addAttribute(TypeAttribute.class);
    }

    int length() {
      return this.posLenAtt.getPositionLength();
    }

    int posInc() {
      return this.posIncAtt.getPositionIncrement();
    }

    CharSequence term() {
      return this.termAtt;
    }

    String type() {
      return this.typeAtt.type();
    }

    int startOffset() {
      return this.offsetAtt.startOffset();
    }

    int endOffset() {
      return this.offsetAtt.endOffset();
    }

    void reset(AttributeSource attSource) {
      this.attSource.restoreState(attSource.captureState());
      this.nextToken = null;
    }

    @Override
    public String toString() {
      return term() + "(" + startOffset() + "," + endOffset() + ") " + posInc() + "," + length();
    }
  }

}
