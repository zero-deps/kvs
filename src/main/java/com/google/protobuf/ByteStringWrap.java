package com.google.protobuf;

import com.google.protobuf.ByteString;

public class ByteStringWrap {
  public static ByteString wrap(byte[] bytes) {
    return ByteString.wrap(bytes);
  }
}
