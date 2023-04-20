package com.germainsoftware.elasticsearch.aggregations;

import com.germainsoftware.elasticsearch.DigestByteMapper;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.xcontent.XContentBuilder;

public class InternalDigest extends InternalAggregation implements Digest {

    public static final double DIGEST_COMPRESSION = 100.0;
    private final MergingDigest digest;

    public InternalDigest(String name, MergingDigest digest, Map<String, Object> metadata) {
        super(name, metadata);
        this.digest = digest;
    }

    /**
     * Read from a stream.
     * @param in
     * @throws java.io.IOException
     */
    public InternalDigest(StreamInput in) throws IOException {
        super(in);
        final var arr = in.readByteArray();
        if (arr != null && arr.length > 0) {
            digest = DigestByteMapper.fromByteArray(arr);
        } else {
            digest = new MergingDigest(DIGEST_COMPRESSION);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        final var arr = DigestByteMapper.toByteArray(digest);
        out.writeByteArray(arr != null ? arr : new byte[0]);
    }

    @Override
    public MergingDigest getValue() {
        return digest;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1 && "value".equals(path.get(0))) {
            return digest;
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }
    
    @Override
    public String getWriteableName() {
        return DigestAggregationBuilder.NAME;
    }

    @Override
    public InternalDigest reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        final var newDigest = new MergingDigest(DIGEST_COMPRESSION);
        for (InternalAggregation aggregation : aggregations) {
            final var other = ((InternalDigest)aggregation).digest;
            if (other != null) {
                newDigest.add(other);
            }
        }
        newDigest.compress();
        return new InternalDigest(getName(), newDigest, getMetadata());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), digest.size() != 0 ? DigestByteMapper.toByteArray(digest) : null);
        return builder;
    }
    
    @Override
    public final double sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        throw new IllegalArgumentException("Digest aggregations cannot have sub-aggregations (at [>" + head + "]");
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), digest);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;
        final var other = (InternalDigest) obj;
        return Objects.equals(digest, other.digest);
    }
}