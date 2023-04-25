package com.germainsoftware.elasticsearch.aggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

public class RawDigestAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final MetricAggregatorSupplier aggregatorSupplier;

    RawDigestAggregatorFactory(
            String name,
            ValuesSourceConfig config,
            AggregationContext context,
            AggregatorFactory parent,
            AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metadata,
            MetricAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.aggregatorSupplier = aggregatorSupplier;
    }

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
                RawDigestAggregationBuilder.REGISTRY_KEY,
                List.of(CoreValuesSourceType.NUMERIC),
                RawDigestAggregator::new,
                true
        );
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new RawDigestAggregator(name, config, context, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
            throws IOException {
        return aggregatorSupplier.build(name, config, context, parent, metadata);
    }
}