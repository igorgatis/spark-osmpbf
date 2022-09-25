// This software is released into the Public Domain.  See copying.txt for details.
package com.google.protobuf;

public final class ByteStringWrapper {

  private ByteStringWrapper() {
  }

  public static ByteString wrap(byte[] bytes) {
    return ByteString.wrap(bytes);
  }
}
