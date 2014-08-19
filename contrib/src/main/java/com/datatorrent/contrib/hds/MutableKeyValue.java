/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.hds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Arrays;


/**
 * Simple convenience wrapper for mutable key/value byte array tuple.
 */
public class MutableKeyValue {
  private byte key[];
  private byte value[];
  public MutableKeyValue(byte[] key, byte[] value) {
    this.key = key;
    this.value = value;
  }

  public byte[] getKey() {
    return key;
  }

  public void setKey(byte[] key) {
    this.key = key;
  }

  public byte[] getValue() {
    return value;
  }

  public void setValue(byte[] value) {
    this.value = value;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof MutableKeyValue)) return false;

    MutableKeyValue mutableKeyValue = (MutableKeyValue) o;

    if (!Arrays.equals(key, mutableKeyValue.key)) return false;
    if (!Arrays.equals(value, mutableKeyValue.value)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = Arrays.hashCode(key);
    result = 31 * result + Arrays.hashCode(value);
    return result;
  }

  private static class MutableKeyValSerializer implements HDS.WalSerializer<MutableKeyValue> {

    @Override public byte[] toBytes(MutableKeyValue data)
    {
      byte[] key = data.getKey();
      byte[] val = data.getValue();
      int keyLen = key.length;
      int valLen = val.length;

      ByteArrayOutputStream out = new ByteArrayOutputStream();
      out.write(keyLen);
      out.write(key, 0, keyLen);
      out.write(valLen);
      out.write(val, 0, valLen);
      return out.toByteArray();
    }

    @Override public MutableKeyValue fromBytes(byte[] arr)
    {
      ByteArrayInputStream bin = new ByteArrayInputStream(arr);
      int keyLen = bin.read();
      byte[] key = new byte[keyLen];
      bin.read(key, 0, keyLen);
      int valLen = bin.read();
      byte[] val = new byte[valLen];
      bin.read(val, 0, valLen);
      return new MutableKeyValue(key, val);
    }
  }

  public static final HDS.WalSerializer<MutableKeyValue> DEFAULT_SERIALIZER = new MutableKeyValSerializer();
}