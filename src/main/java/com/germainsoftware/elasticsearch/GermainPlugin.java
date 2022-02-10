package com.germainsoftware.elasticsearch;

import com.germainsoftware.elasticsearch.aggregations.DigestAggregationBuilder;
import com.germainsoftware.elasticsearch.aggregations.InternalDigest;
import static java.util.Collections.singletonList;
import java.util.List;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

public class GermainPlugin extends Plugin implements SearchPlugin {

    @Override
    public List<AggregationSpec> getAggregations() {
        return singletonList(
            new AggregationSpec(DigestAggregationBuilder.NAME, 
                    DigestAggregationBuilder::new, 
                    DigestAggregationBuilder.PARSER)
                .addResultReader(InternalDigest::new)
                .setAggregatorRegistrar(DigestAggregationBuilder::registerAggregators)
        );
    }
}