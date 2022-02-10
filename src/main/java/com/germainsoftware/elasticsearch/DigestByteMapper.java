package com.germainsoftware.elasticsearch;

import com.tdunning.math.stats.MergingDigest;
import java.nio.Buffer;

import java.nio.ByteBuffer;
import java.util.Base64;

public class DigestByteMapper {
    
    private DigestByteMapper() {
        throw new IllegalStateException();
    }
    
    public static MergingDigest fromByteArray(byte[] bytes){
        return fromByteArray(bytes, 0, bytes.length);
    }

    public static MergingDigest fromByteArray(byte[] bytes, int offset, int length) {
        final var byteBuffer = ByteBuffer.wrap(new byte[length]);
        byteBuffer.put(bytes, offset, length);
        // Fix to make this compile on JDK11 and run on JDK8
        ((Buffer)byteBuffer).flip();
        return MergingDigest.fromBytes(byteBuffer);
    }
    
    public static MergingDigest fromBase64String(String s) {
        final var bytes = Base64.getDecoder().decode(s);
        return fromByteArray(bytes);
    }
    
    public static byte[] toByteArray(MergingDigest digest){
        final var byteSize = digest.byteSize();
        final var byteBuffer = ByteBuffer.allocate(byteSize);
        digest.asBytes(byteBuffer);
        final var digestBytes = new byte[byteSize];
        // Fix to make this compile on JDK11 and run on JDK8
        ((Buffer)byteBuffer).flip();
        byteBuffer.get(digestBytes, 0, byteSize);
        return digestBytes;
    }
}
