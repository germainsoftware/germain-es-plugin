package com.germainsoftware.elasticsearch.aggregations;

import com.germainsoftware.elasticsearch.DigestByteMapper;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

public class InternalDigest extends InternalAggregation implements Digest {

    private final MergingDigest digest;

    public InternalDigest(String name, MergingDigest digest, Map<String, Object> metadata) {
        super(name, metadata);
        if (digest == null) {
            throw new IllegalArgumentException("Digest was null");
        }
        this.digest = digest;
    }

    /**
     * Read from a stream.
     * @param in
     * @throws java.io.IOException
     */
    public InternalDigest(StreamInput in) throws IOException {
        super(in);
        final var compression = in.readDouble();
        final var arr = in.readByteArray();
        if (arr != null && arr.length > 0) {
            digest = DigestByteMapper.fromByteArray(arr);
            if (digest == null) {
                throw new IllegalArgumentException("Digest from stream was null");
            }
        } else {
            digest = new MergingDigest(compression);
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(digest.compression());
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
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext arc, int i) {
        return new AggregatorReducer() {
            MergingDigest merged = new MergingDigest(digest.compression());

            @Override
            public void accept(InternalAggregation aggregation) {
                final var other = ((InternalDigest)aggregation).digest;
                if (other != null) {
                    merged.add(other);
                }
            }

            @Override
            public InternalAggregation get() {
                merged.compress();
                return new InternalDigest(getName(), merged, getMetadata());
            }
        };
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.VALUE.getPreferredName(), digest.size() != 0 ? DigestByteMapper.toByteArray(digest) : null);
        return builder;
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