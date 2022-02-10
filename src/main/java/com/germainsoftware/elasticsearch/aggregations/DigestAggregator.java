package com.germainsoftware.elasticsearch.aggregations;

import com.germainsoftware.elasticsearch.DigestByteMapper;
import com.germainsoftware.elasticsearch.GermainLogger;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
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
            final var bigArrays = context.bigArrays();
            digests = bigArrays.newObjectArray(1);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        return valuesSource != null && valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        GermainLogger.log("DigestAggregator.getLeafCollector");
        if (valuesSource == null) {
            GermainLogger.log("DigestAggregator.getLeafCollector - NO OP");
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }
        final SortedBinaryDocValues values = valuesSource.bytesValues(ctx);
        GermainLogger.log("DigestAggregator.getLeafCollector - values: " + values.docValueCount());

        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                GermainLogger.log("DigestAggregator.collect: " + doc + " " + bucket);

                digests = bigArrays().grow(digests, bucket + 1);

                if (values.advanceExact(doc)) {
                    final int valueCount = values.docValueCount();
                    var digest = digests.get(bucket);
                    for (int i = 0; i < valueCount; i++) {
                        final var value = values.nextValue();
                        final var digestValue = DigestByteMapper.fromByteArray(value.bytes, value.offset, value.length);
                        GermainLogger.log("Load - p95: " + digestValue.quantile(0.95));
                        if (digest == null) {
                            digest = digestValue;
                        } else {
                            digest.add(digestValue);
                        }
                    }
                    GermainLogger.log("Set - p95: " + digest.quantile(0.95));
                    digests.set(bucket, digest);
                }
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        GermainLogger.log("DigestAggregator.buildAggregation: " + bucket);
        if (valuesSource == null || bucket >= digests.size()) {
            return buildEmptyAggregation();
        }
        return new InternalDigest(name, digests.get(bucket), metadata());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        GermainLogger.log("DigestAggregator.buildEmptyAggregation");
        return new InternalDigest(name, new MergingDigest(10), metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(digests);
    }
}
