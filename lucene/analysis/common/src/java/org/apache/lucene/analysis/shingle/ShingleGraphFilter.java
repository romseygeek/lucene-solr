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
import java.util.Arrays;
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

  private static final Token END_TOKEN = new Token(new AttributeSource());

  private final List<Token> tokenPool = new LinkedList<>();

  private final int minShingleSize;
  private final int maxShingleSize;
  private final boolean emitUnigrams;
  private final String tokenSeparator;
  private final Token GAP_TOKEN = new Token(new AttributeSource());

  private final PositionLengthAttribute lenAtt = addAttribute(PositionLengthAttribute.class);
  private final PositionIncrementAttribute incAtt = addAttribute(PositionIncrementAttribute.class);
  private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
  private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
  private final TypeAttribute typeAtt = addAttribute(TypeAttribute.class);

  private Token[] tokens;
  private int[] tokenStackSize;
  private int[] currentStackPos;
  private int shingleSize;
  private boolean unigramDone;

  public ShingleGraphFilter(TokenStream input, int minShingleSize, int maxShingleSize, boolean emitUnigrams, String tokenSeparator, String fillerToken) {
    super(input);
    this.minShingleSize = minShingleSize;
    this.maxShingleSize = maxShingleSize;
    this.emitUnigrams = emitUnigrams;
    this.tokenSeparator = tokenSeparator;

    this.GAP_TOKEN.termAtt.setEmpty().append(fillerToken);

    this.tokens = new Token[maxShingleSize];
    this.tokenStackSize = new int[maxShingleSize];   // number of stacked tokens at given position in the shingle
    this.currentStackPos = new int[maxShingleSize];  // current depth in the token stack for each position in the shingle
  }

  @Override
  public boolean incrementToken() throws IOException {
    int posInc = 0;
    if (nextShingle() == false) {
      Token nextRoot = nextToken(tokens[0]);
      if (nextRoot == END_TOKEN)
        return false;
      recycleToken(tokens[0]);
      reset(nextRoot);
      posInc = tokens[0].posInc();
    }
    clearAttributes();
    lenAtt.setPositionLength(shingleSize);
    incAtt.setPositionIncrement(posInc);
    offsetAtt.setOffset(tokens[0].startOffset(), lastToken().endOffset());
    termAtt.setEmpty();
    termAtt.append(tokens[0].term());
    typeAtt.setType(shingleSize > 1 ? "shingle" : tokens[0].type());
    for (int i = 1; i < shingleSize; i++) {
      termAtt.append(tokenSeparator).append(tokens[i].term());
    }
    return true;
  }

  @Override
  public void reset() throws IOException {
    super.reset();
    this.tokens[0] = null;
  }

  private Token lastToken() {
    int lastTokenIndex = shingleSize - 1;
    while (tokens[lastTokenIndex] == GAP_TOKEN) {
      lastTokenIndex--;
    }
    return tokens[lastTokenIndex];
  }

  private void reset(Token token) throws IOException {
    this.tokens[0] = token;
    this.tokenStackSize[0] = 0;   // other tokens at this position are dealt with in the next shingle
    this.shingleSize = maxShingleSize;
    this.unigramDone = !emitUnigrams;
    Arrays.fill(this.currentStackPos, 0);
    for (int i = 1; i < maxShingleSize; i++) {
      Token current = this.tokens[i - 1];
      int length = current.length();
      // skip to first token {length} positions down the line
      while (length > 0) {
        current = nextToken(current);
        if (current == END_TOKEN) {
          this.shingleSize = i;
          return;
        }
        length -= current.posInc();
      }
      if (current.posInc() > 1) {
        // insert gaps into the shingle list
        for (int j = 1; j < current.posInc(); j++) {
          this.tokens[i] = GAP_TOKEN;
          i++;
        }
        if (i >= maxShingleSize) {
          break;
        }
      }
      this.tokens[i] = current;
      // get depth of token stack at this position
      tokenStackSize[i] = 0;
      for (Token t = current; t.posInc() == 0; t = t.nextToken) {
        tokenStackSize[i]++;
      }
    }
  }

  private boolean nextShingle() {
    if (tokens[0] == null)
      return false;
    if (shingleSize <= minShingleSize) {
      if (hasStackedTokens()) {
        advanceStack();
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

  private boolean hasStackedTokens() {
    for (int i = 1; i < maxShingleSize; i++) {
      if (currentStackPos[i] < tokenStackSize[i]) {
        return true;
      }
    }
    return false;
  }

  private void advanceStack() {
    for (int i = maxShingleSize; i >= 1; i--) {
      if (currentStackPos[i] < tokenStackSize[i]) {
        currentStackPos[i]++;
        return;
      }
      currentStackPos[i] = 0;
    }
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
    if (tokens[0] == END_TOKEN || tokens[0] == null)
      return tokenCount;
    for (Token t = tokens[0]; t != END_TOKEN && t != null; t = t.nextToken) {
      tokenCount++;
    }
    return tokenCount;
  }

  private Token nextToken(Token token) throws IOException {
    if (token == null) {
      if (input.incrementToken() == false) {
        return END_TOKEN;
      }
      return newToken();
    }
    if (token.nextToken == null) {
      if (input.incrementToken() == false) {
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
  }

  private class Shingle {

  }

}
