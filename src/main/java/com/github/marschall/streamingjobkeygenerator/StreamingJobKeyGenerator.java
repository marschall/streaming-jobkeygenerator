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
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

  @Override
  public String generateKey(JobParameters source) {
    Assert.notNull(source, "source must not be null");
    Map<String, JobParameter> props = source.getParameters();

    IncrementalHasher hasher = new IncrementalHasher();
    try {
      List<String> keys = new ArrayList<>(props.keySet());
      Collections.sort(keys);
      for (String key : keys) {
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
      return hasher.digest();
    } catch (IOException | DigestException e) {
      throw new IllegalStateException("Failed to compute MD-5 hash", e);
    }

  }

  /**
   * Performs incremental UTF-8 encoding and MD5 hashing.
   */
  static final class IncrementalHasher {
    
    private static final int MD5_LENGTH = 16;

    private final CharBuffer charBuffer;

    private final ByteBuffer byteBuffer;

    private MessageDigest messageDigest;

    private CharsetEncoder encoder;

    IncrementalHasher() {
      this.charBuffer = CharBuffer.allocate(64);
      this.byteBuffer = ByteBuffer.allocate(64);
      this.encoder = StandardCharsets.UTF_8
          .newEncoder()
          .onMalformedInput(REPLACE);
      try {
        this.messageDigest = MessageDigest.getInstance("MD5");
      } catch (NoSuchAlgorithmException e) {
        throw new IllegalStateException("MD5 algorithm not available.  Fatal (should be in the JDK).", e);
      }
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
