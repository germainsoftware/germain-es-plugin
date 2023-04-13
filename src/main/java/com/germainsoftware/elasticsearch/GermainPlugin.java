package com.germainsoftware.elasticsearch;

import com.germainsoftware.elasticsearch.aggregations.DigestAggregationBuilder;
import com.germainsoftware.elasticsearch.aggregations.RawDigestAggregationBuilder;
import com.germainsoftware.elasticsearch.aggregations.InternalDigest;
import java.util.Arrays;
import java.util.List;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

public class GermainPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return Arrays.asList(
            new AggregationSpec(DigestAggregationBuilder.NAME, 
                    DigestAggregationBuilder::new, 
                    DigestAggregationBuilder.PARSER)
                .addResultReader(InternalDigest::new)
                .setAggregatorRegistrar(DigestAggregationBuilder::registerAggregators),
            new AggregationSpec(RawDigestAggregationBuilder.NAME, 
                    RawDigestAggregationBuilder::new, 
                    RawDigestAggregationBuilder.PARSER)
                .addResultReader(InternalDigest::new)
                .setAggregatorRegistrar(RawDigestAggregationBuilder::registerAggregators)
        );
    }
}