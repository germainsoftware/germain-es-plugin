package com.germainsoftware.elasticsearch.aggregations;

import com.germainsoftware.elasticsearch.DigestByteMapper;
import static com.germainsoftware.elasticsearch.aggregations.InternalDigest.DIGEST_COMPRESSION;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

public class DigestAggregator extends MetricsAggregator {

    private final ValuesSource.Bytes valuesSource;
    private ObjectArray<MergingDigest> digests;

    DigestAggregator(String name, ValuesSourceConfig valuesSourceConfig, AggregationContext context,
            Aggregator parent, Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);

        this.valuesSource = valuesSourceConfig.hasValues() ? (ValuesSource.Bytes) valuesSourceConfig.getValuesSource() : null;
        if (valuesSource != null) {
            digests = context.bigArrays().newObjectArray(1);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final var values = valuesSource.bytesValues(ctx);

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                digests = bigArrays().grow(digests, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    var digest = digests.get(bucket);
                    for (int i = 0; i < valueCount; i++) {
                        final var value = values.nextValue();
                        final var digestValue = DigestByteMapper.fromByteArray(value.bytes, value.offset, value.length);
                        if (digest == null) {
                            digest = digestValue;
                        } else {
                            digest.add(digestValue);
                        }
                    }
                    digests.set(bucket, digest);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSource == null || bucket >= digests.size()) {
            return buildEmptyAggregation();
        }
        return new InternalDigest(name, digests.get(bucket), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalDigest(name, new MergingDigest(DIGEST_COMPRESSION), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(digests);
    }
}
