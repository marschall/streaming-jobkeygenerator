package com.github.marschall.streamingjobkeygenerator;

import static java.nio.charset.CodingErrorAction.REPLACE;
import static java.nio.charset.StandardCharsets.ISO_8859_1;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.security.DigestException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Map;

import org.springframework.batch.core.DefaultJobKeyGenerator;
import org.springframework.batch.core.JobKeyGenerator;
import org.springframework.batch.core.JobParameter;
import org.springframework.batch.core.JobParameters;
import org.springframework.util.Assert;

/**
 * A replacement for {@link DefaultJobKeyGenerator} that avoids large
 * intermediate allocations.
 * <p>
 * This class is a drop in replacement for {@link DefaultJobKeyGenerator}
 * and produces the same output.
 * 
 * @see DefaultJobKeyGenerator
 */
public final class StreamingJobKeyGenerator implements JobKeyGenerator<JobParameters> {

  /**
   * Job key for empty job parameters.
   */
  private static final String EMPTY_JOB_PARAMETERS_KEY = "d41d8cd98f00b204e9800998ecf8427e";

  @Override
  public String generateKey(JobParameters source) {
    Assert.notNull(source, "source must not be null");
    if (source.isEmpty()) {
      return EMPTY_JOB_PARAMETERS_KEY;
    }

    Map<String, JobParameter> props = source.getParameters();

    try {
      if (props.size() == 1) {
        String key = props.keySet().iterator().next();
        return hashSingleParameter(key, props.get(key));
      } else {
        IncrementalHasher hasher = new IncrementalHasher();
        // using String[] instead of ArrayList avoids one Object[] copy
        // the new String[0] allocation should not show up
        // https://shipilev.net/blog/2016/arrays-wisdom-ancients/
        String[] keys = props.keySet().toArray(new String[0]);
        Arrays.sort(keys, String::compareTo);
        for (String key : keys) {
          hashParameter(key, props, hasher);
        }
        return hasher.digest();
      }
    } catch (IOException | DigestException e) {
      throw new IllegalStateException("Failed to compute MD-5 hash", e);
    }

  }

  private String hashSingleParameter(String key, JobParameter jobParameter) throws DigestException, IOException {
    if (!jobParameter.isIdentifying()) {
      return EMPTY_JOB_PARAMETERS_KEY;
    }
    Object value = jobParameter.getValue();
    String stringValue = value != null ? value.toString() : null;
    int inputLengthEstimate = key.length() + (stringValue != null ? stringValue.length() : 0) + 2;
    IncrementalHasher hasher = new IncrementalHasher(inputLengthEstimate);
    hasher.put(key);
    hasher.put('=');
    if (stringValue != null) {
      hasher.put(stringValue);
    }
    hasher.put(';');
    return hasher.digest();
  }

  private void hashParameter(String key, Map<String, JobParameter> props, IncrementalHasher hasher)
      throws IOException, DigestException {
    JobParameter jobParameter = props.get(key);
    if(jobParameter.isIdentifying()) {
      hasher.put(key);
      hasher.put('=');
      Object value = jobParameter.getValue();
      if (value != null) {
        hasher.put(jobParameter.toString());
      }
      hasher.put(';');
    }
  }

  /**
   * Performs incremental UTF-8 encoding and MD5 hashing.
   */
  static final class IncrementalHasher {

    private static final int MAX_BUFFER_SIZE = 64;

    private static final int MD5_LENGTH = 16;

    // MD5 hash plus MD5 hash in hex
    private static final int MIN_BYTE_BUFFER_SIZE = MD5_LENGTH + MD5_LENGTH * 2;

    private final CharBuffer charBuffer;

    private final ByteBuffer byteBuffer;

    private MessageDigest messageDigest;

    private CharsetEncoder encoder;

    private IncrementalHasher(int charBufferSize, int bytBufferSize) {
      this.charBuffer = CharBuffer.allocate(charBufferSize);
      this.byteBuffer = ByteBuffer.allocate(bytBufferSize);
      this.encoder = StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(REPLACE);
      try {
        this.messageDigest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).", e);
      }
    }

    IncrementalHasher() {
      this(MAX_BUFFER_SIZE, MAX_BUFFER_SIZE);
    }

    IncrementalHasher(int inputLengthEstimate) {
      this(calculateCharBufferSize(inputLengthEstimate), calculateByteBufferSize(inputLengthEstimate));
    }

    private static int calculateCharBufferSize(int inputLengthEstimate) {
      return Math.min(inputLengthEstimate, MAX_BUFFER_SIZE);
    }

    private static int calculateByteBufferSize(int inputLengthEstimate) {
      return Math.max(Math.min(inputLengthEstimate, MAX_BUFFER_SIZE), MIN_BYTE_BUFFER_SIZE);
    }

    void put(String s) throws IOException, DigestException {
      if (s.isEmpty()) {
        return;
      }
      int length = s.length();
      int remaining = length;
      while (remaining > 0) {
        if (!this.charBuffer.hasRemaining()) {
          this.encodeAndHash(false);
        }
        int toWrite = Math.min(remaining, this.charBuffer.remaining());
        int start = length - remaining;
        this.charBuffer.put(s, start, start + toWrite);
        remaining -= toWrite;
      }
    }

    void put(char c) throws IOException, DigestException {
      if (!this.charBuffer.hasRemaining()) {
        this.encodeAndHash(false);
      }
      this.charBuffer.put(c);
    }

    private void encodeAndHash(boolean end) throws IOException, DigestException {
      this.charBuffer.flip();
      while (this.charBuffer.hasRemaining()) {
        CoderResult result = this.encoder.encode(this.charBuffer, this.byteBuffer, end);
        if (result == CoderResult.OVERFLOW) {
          this.hashBuffer();
        } else if (result == CoderResult.UNDERFLOW) {
          // underflow is signaled when
          // - input buffer has been completely consumed
          // - if the input buffer is not yet empty, that additional input is required
          if (end && this.charBuffer.hasRemaining()) {
            // the encoder expects input no more input is available
            result.throwException();
          }
          if (this.byteBuffer.position() > 0) {
            this.hashBuffer();
          }
        } else if (result.isError()) {
          result.throwException();
        }
      }
      this.charBuffer.compact();
    }

    private void hashBuffer() throws DigestException {
      this.messageDigest.update(this.byteBuffer.array(), 0, this.byteBuffer.position());
      this.byteBuffer.clear();
    }

    private void finish() throws DigestException, IOException {
      this.encodeAndHash(true);
    }

    String digest() throws DigestException, IOException {
      this.finish();
      // we no longer the byte[] of #byteBuffer
      // it has length 64
      // store the MD5 hash in the first 16 bytes
      // store the hex ASCII string in the next 32 bytes
      int digestLength = this.messageDigest.digest(this.byteBuffer.array(), 0, MD5_LENGTH);
      if (digestLength != MD5_LENGTH) {
        throw new DigestException("unexpected hash length");
      }
      return hexEncode();
    }

    private String hexEncode() {
      byte[] array = this.byteBuffer.array();
      // MD5 hash is in the first 16 bytes 
      for (int i = 0; i < MD5_LENGTH; i++) {
        byte b = array[i];
        array[MD5_LENGTH + (i * 2)] = (byte) Character.forDigit((b >>> 4) & 0xF, 16);
        array[MD5_LENGTH + (i * 2 + 1)] = (byte) Character.forDigit(b & 0xF, 16);

      }
      // use the next 32 bytes as a work buffer for the hex ASCII string
      // Strictly speaking US-ASCII but vectorized fast path is only available on JDK 17+
      return new String(array, MD5_LENGTH, MD5_LENGTH * 2, ISO_8859_1);
    }

  }

}
