package io.cdcbench.connect.optimized;

import java.util.Objects;

/** An owned PostgreSQL UTF-8 text value that has not been materialized as a Java String. */
record RawUtf8String(byte[] bytes) {
    RawUtf8String {
        Objects.requireNonNull(bytes, "bytes");
    }
}
