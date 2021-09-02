StreamingJobKeyGenerator
========================

A faster replacement for `DefaultJobKeyGenerator` that incrementally creates the job key and avoid large intermediate allocations.

The code runs noticeably faster in JDK 17 compared to JDK 11, so does `DefaultJobKeyGenerator` as well.


