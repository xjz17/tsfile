/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.common.ExperimentalLongCodec;
import org.apache.tsfile.encoding.common.ExperimentalNumericPageCodec;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.encoding.TsFileDecodingException;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.nio.ByteBuffer;

/** Shared page decoder for the experimental BOS and Sub-column numeric encodings. */
abstract class ExperimentalNumericDecoder extends Decoder {

  private final ExperimentalLongCodec.Kind kind;
  private final TSDataType dataType;
  private long[] values;
  private int position;

  ExperimentalNumericDecoder(
      TSEncoding encoding, ExperimentalLongCodec.Kind kind, TSDataType dataType) {
    super(encoding);
    this.kind = kind;
    this.dataType = dataType;
  }

  @Override
  public int readInt(ByteBuffer buffer) {
    return (int) next(buffer);
  }

  @Override
  public long readLong(ByteBuffer buffer) {
    return next(buffer);
  }

  @Override
  public float readFloat(ByteBuffer buffer) {
    return Float.intBitsToFloat((int) next(buffer));
  }

  @Override
  public double readDouble(ByteBuffer buffer) {
    return Double.longBitsToDouble(next(buffer));
  }

  private long next(ByteBuffer buffer) {
    ensureDecoded(buffer);
    if (position >= values.length) {
      throw new TsFileDecodingException("no more values in experimental numeric page");
    }
    return values[position++];
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    ensureDecoded(buffer);
    return position < values.length;
  }

  private void ensureDecoded(ByteBuffer buffer) {
    if (values != null) {
      return;
    }
    byte[] payload = new byte[buffer.remaining()];
    buffer.get(payload);
    try {
      values = ExperimentalNumericPageCodec.decode(kind, dataType, payload);
    } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
      throw new TsFileDecodingException("invalid experimental numeric page: " + e.getMessage());
    }
  }

  @Override
  public void reset() {
    values = null;
    position = 0;
  }
}
