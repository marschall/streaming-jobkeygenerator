StreamingJobKeyGenerator
========================

A faster replacement for `DefaultJobKeyGenerator` that incrementally creates the job key and avoid large intermediate allocations.

The code runs noticeably faster in JDK 17 compared to JDK 11, so does `DefaultJobKeyGenerator` as well.

See [marschall/jobkeygenerator-benchmarks](https://github.com/marschall/jobkeygenerator-benchmarks) for benchmarks


Implementation Notes
--------------------

`CharsetEncoder` small `CharBuffer` and small `ByteBuffer`

    String -> CharBuffer -(CharsetEncoder)-> ByteBuffer -> MessageDigest

`OutputStreamWriter` `StreamEncoder` with a `byte[8192]`

Avoid `BigInteger` and `String#format` for hex printing.

The code benefits from the following improvements in JDK 11:

- [JDK-8054307](https://bugs.openjdk.java.net/browse/JDK-8054307)

The code benefits from the following improvements in JDK 17:

- [JDK-8011135](https://bugs.openjdk.java.net/browse/JDK-8011135)
- [JDK-8250902](https://bugs.openjdk.java.net/browse/JDK-8250902)
- [JDK-8259498](https://bugs.openjdk.java.net/browse/JDK-8259498)
- [JDK-8259065](https://bugs.openjdk.java.net/browse/JDK-8259065)

