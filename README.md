StreamingJobKeyGenerator
========================

A faster replacement for `DefaultJobKeyGenerator` that incrementally creates the job key and avoid large intermediate allocations.


See [marschall/jobkeygenerator-benchmarks](https://github.com/marschall/jobkeygenerator-benchmarks) for benchmarks


Implementation Notes
--------------------

- The code uses a small, limited size, `CharBuffer` to incrementally build the key and a small, limited size, `ByteBuffer` to hold the incremental input for the `MessageDigest`. A `CharsetEncoder` is used for the UTF-8 encoding from `CharBuffer` to the `ByteBuffer`.

      String -> CharBuffer -(CharsetEncoder)-> ByteBuffer -> MessageDigest

- The code avoids the generating the full key as a `String` using a `StringBuilder` but instead uses a limited size `CharBuffer` work buffer to incrementally build the key.

- The code avoids converting the full key to UTF-8 into a single, potentially large, `byte[]` but instead uses a limited size `ByteBuffer` work buffer to incrementally build the hash input.

- The code uses a `CharsetEncoder` to avoid creating a `OutputStreamWriter` instance to avoid creating a `byte[8192]`buffer.

- The code avoids `BigInteger` and `String#format` for hex printing. Instead it implements a hex printer that reuses the existing `ByteBuffer` as a work buffer to avoid intermediate allocations.

- The code runs noticeably faster in JDK 17 compared to JDK 11, so does `DefaultJobKeyGenerator` as well.

  - It benefits from the following improvements in JDK 11:

    - [JDK-8054307](https://bugs.openjdk.java.net/browse/JDK-8054307)

  - It benefits from the following improvements in JDK 17:

    - [JDK-8011135](https://bugs.openjdk.java.net/browse/JDK-8011135)
    - [JDK-8250902](https://bugs.openjdk.java.net/browse/JDK-8250902)
    - [JDK-8259498](https://bugs.openjdk.java.net/browse/JDK-8259498)
    - [JDK-8259065](https://bugs.openjdk.java.net/browse/JDK-8259065)

