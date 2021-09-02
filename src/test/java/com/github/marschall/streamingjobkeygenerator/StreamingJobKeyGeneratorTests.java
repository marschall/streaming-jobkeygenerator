package com.github.marschall.streamingjobkeygenerator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;

class StreamingJobKeyGeneratorTests {

  private JobKeyGenerator<JobParameters> implementation;
  private JobKeyGenerator<JobParameters> reference;

  @BeforeEach
  void setUp() {
    this.implementation = new StreamingJobKeyGenerator();
    this.reference = new DefaultJobKeyGenerator();
  }

  static List<JobParameters> jobParameters() {
    JobParameters empty = new JobParameters();

    JobParameters order1 = new JobParametersBuilder()
        .addString("key1", "value1")
        .addString("key2", "value2")
        .toJobParameters();

    JobParameters order2 = new JobParametersBuilder()
        .addString("key2", "value2")
        .addString("key1", "value1")
        .toJobParameters();

    JobParameters nonIdentifying = new JobParametersBuilder()
        .addString("identifying", "true")
        .addString("nonIdentifying", "false", false)
        .toJobParameters();

    JobParameters allTypes = new JobParametersBuilder()
        .addString("string", "string")
        .addDate("date", new Date(1L))
        .addDouble("double", 1.0d)
        .addLong("long", 1L)
        .toJobParameters();

    JobParameters nullValues = new JobParametersBuilder()
        .addString("string", null)
        .addDate("date", null)
        .addDouble("double", null)
        .addLong("long", null)
        .toJobParameters();

    JobParameters encodings = new JobParametersBuilder()
        .addString("ASCII", "ASCII")
        .addString("ISO-8859-1", "\u00F6")
        .addString("BMP", "\u010D")
        .addString("SMP", "\ud83d\ude08")
        .toJobParameters();

    JobParameters emptyStrings = new JobParametersBuilder()
        .addString("", "")
        .toJobParameters();

    JobParameters edgeCases1 = new JobParametersBuilder()
        .addString(stringOfLength(63), "string-length-63")
        .toJobParameters();

    JobParameters edgeCases2 = new JobParametersBuilder()
        .addString(stringOfLength(64), "string-length-64")
        .toJobParameters();

    JobParameters edgeCases3 = new JobParametersBuilder()
        .addString(stringOfLength(65), "string-length-65")
        .toJobParameters();

    JobParameters edgeCases4 = new JobParametersBuilder()
        .addString(stringOfLength(63) + "\u00F6", "char-length-64-byte-length-65")
        .toJobParameters();
    
    JobParameters edgeCases5 = new JobParametersBuilder()
        .addString("\u00F6" + stringOfLength(63), "char-length-64-byte-length-65")
        .toJobParameters();
    
    JobParameters malformed = new JobParametersBuilder()
        .addString("\ude08", "malformed")
        .toJobParameters();

    return List.of(empty, order1, order2, nonIdentifying, allTypes, nullValues, encodings, emptyStrings,
        edgeCases1, edgeCases2, edgeCases3, edgeCases4, edgeCases5, malformed);
  }
  
  private static String stringOfLength(int length) {
    StringBuilder buffer = new StringBuilder(length);
    for (int i = 0; i < length; i++) {
     buffer.append((char) ('0' + (i % 10))); 
    }
    return buffer.toString();
  }

  @ParameterizedTest
  @MethodSource("jobParameters")
  void test(JobParameters jobParameters) {
    String expected = this.reference.generateKey(jobParameters);
    String actual = this.implementation.generateKey(jobParameters);
    assertEquals(expected, actual);
  }

}
