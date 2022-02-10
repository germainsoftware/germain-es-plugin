package com.germainsoftware.elasticsearch.aggregations;

import com.tdunning.math.stats.MergingDigest;
import org.elasticsearch.search.aggregations.Aggregation;

public interface Digest extends Aggregation {

    /**
     * Returns the digest.
     * 
     * @return A {@code MergingDigest}.
     */
    MergingDigest getValue();
    
}