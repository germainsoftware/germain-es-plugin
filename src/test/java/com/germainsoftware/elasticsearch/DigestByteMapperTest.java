package com.germainsoftware.elasticsearch;

import com.tdunning.math.stats.MergingDigest;
import java.util.Base64;
import org.junit.Test;

public class DigestByteMapperTest {

    @Test
    public void testMedian() {
        
        
        final var tmp1 = Base64.getDecoder().decode("AAAAAT/E3S8an753P8euFHrhR65AJAAAAAAAAAAAAAI/8AAAAAAAAD/E3S8an753P/AAAAAAAAA/x64UeuFHrg==");
        final var digest1 = DigestByteMapper.fromByteArray(tmp1);
        final var tmp2 = Base64.getDecoder().decode("AAAAAT+xaHKwIMScP+mJN0vGp/BAJAAAAAAAAAAAAAI/8AAAAAAAAD+xaHKwIMScP/AAAAAAAAA/6Yk3S8an8A==");
        final var digest2 = DigestByteMapper.fromByteArray(tmp2);

        MergingDigest digest = new MergingDigest(10);
        digest.add(digest1);
        digest.add(digest2);
        digest.compress();
        
        // assertEquals(0.433, digest.quantile(0.5), 0.001);
        
    }
    
}
