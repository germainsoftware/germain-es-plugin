package com.germainsoftware.elasticsearch.aggregations;

import com.germainsoftware.elasticsearch.DigestByteMapper;
import com.tdunning.math.stats.MergingDigest;
import java.io.IOException;
import org.elasticsearch.search.aggregations.ParsedAggregation;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ObjectParser.ValueType;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

public class ParsedDigest extends ParsedAggregation implements Digest {

    private MergingDigest value;
    
    @Override
    public MergingDigest getValue() {
        return value;
    }
    
    public void setValue(String val) {
        if (val != null) {
            value = DigestByteMapper.fromBase64String(val);
        }
    }

    @Override
    public String getType() {
        return DigestAggregationBuilder.NAME;
    }
    
    @Override
    protected XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        // InternalDigest renders value only if the value count is not 0.
        builder.field(CommonFields.VALUE.getPreferredName(), value != null ? DigestByteMapper.toByteArray(value) : null);
        return builder;
    }

    private static final ObjectParser<ParsedDigest, Void> PARSER = new ObjectParser<>(ParsedDigest.class.getSimpleName(), true, ParsedDigest::new);
    
    protected static void declareSingleValueFields(ObjectParser<? extends ParsedDigest, Void> objectParser) {
        declareAggregationFields(objectParser);
        objectParser.declareField(
            ParsedDigest::setValue,
            (parser, context) -> parser.textOrNull(),
            CommonFields.VALUE,
            ValueType.STRING_OR_NULL
        );
    }
    
    static {
        declareSingleValueFields(PARSER);
    }
   
    public static ParsedDigest fromXContent(XContentParser parser, final String name) {
        ParsedDigest digest = PARSER.apply(parser, null);
        digest.setName(name);
        return digest;
    }
}