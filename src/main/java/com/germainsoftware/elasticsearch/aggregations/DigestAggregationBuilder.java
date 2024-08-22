package com.germainsoftware.elasticsearch.aggregations;

import java.io.IOException;
import java.util.Map;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.metrics.MetricAggregatorSupplier;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent.Params;
import org.elasticsearch.xcontent.XContentBuilder;

public class DigestAggregationBuilder extends ValuesSourceAggregationBuilder.SingleMetricAggregationBuilder<DigestAggregationBuilder> {

    public static final String NAME = "digest";
    public static final ValuesSourceRegistry.RegistryKey<MetricAggregatorSupplier> REGISTRY_KEY = new ValuesSourceRegistry.RegistryKey<>(
            NAME,
            MetricAggregatorSupplier.class
    );
    public static final ParseField COMPRESSION_FIELD = new ParseField("compression");

    public static final ObjectParser<DigestAggregationBuilder, String> PARSER
        = ObjectParser.fromBuilder(NAME, DigestAggregationBuilder::new);

    static {
        ValuesSourceAggregationBuilder.declareFields(PARSER, true, true, false);
        PARSER.declareDouble(DigestAggregationBuilder::compression, DigestAggregationBuilder.COMPRESSION_FIELD);
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        DigestAggregatorFactory.registerAggregators(builder);
    }

    // Digest compression factor
    private double compression = 100.0;
    
    public DigestAggregationBuilder(String name) {
        super(name);
    }

    public DigestAggregationBuilder(DigestAggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        super(clone, factoriesBuilder, metadata);
        this.compression = clone.compression;
    }

    public DigestAggregationBuilder(StreamInput in) throws IOException {
        super(in);
        this.compression = in.readDouble();
    }

    @Override
    protected ValuesSourceType defaultValueSourceType() {
        return CoreValuesSourceType.KEYWORD;
    }

    @Override
    protected AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata) {
        return new DigestAggregationBuilder(this, factoriesBuilder, metadata);
    }

    @Override
    protected void innerWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(compression);
    }

    @Override
    protected DigestAggregatorFactory innerBuild(AggregationContext context, ValuesSourceConfig config,
            AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder
    ) throws IOException {
        final var aggregatorSupplier = context.getValuesSourceRegistry().getAggregator(REGISTRY_KEY, config);
        return new DigestAggregatorFactory(name, config, context, parent, subFactoriesBuilder, 
                metadata, aggregatorSupplier, compression);
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field("compression", compression);
        return builder;
    }

    @Override
    public String getType() {
        return NAME;
    }
    
    /**
     * Sets the compression factor used for this digest aggregation.
     * 
     * @param compression The compression factor.
     * @return This builder.
     */
    public DigestAggregationBuilder compression(double compression) {
        this.compression = compression;
        return this;
    }

    /**
     * Gets the compression to use for this aggregation.
     * @return The compression factor.
     */
    public double compression() {
        return compression;
    }
    
    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.ZERO;
    }
}
