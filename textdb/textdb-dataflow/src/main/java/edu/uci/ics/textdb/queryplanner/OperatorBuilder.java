package edu.uci.ics.textdb.queryplanner;

import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

public class OperatorBuilder {
    
    @FunctionalInterface
    public static interface BuildOperator {
        IOperator buildOperator(Map<String, Object> operatorProperties) throws Exception;
    }
    
    IOperator buildKeywordMatcher(Map<String, Object> operatorProperties) throws ParseException {
        String KEYWORD = "keyword";
        String ATTRIBUTES = "attributes";
        String MATCHING_TYPE = "matchingType";
        String LIMIT = "limit";
        String OFFSET = "offset";
        
        KeywordMatcher keywordMatcher = null;
        if (! operatorProperties.containsKey(KEYWORD)) {
            throw new ParseException("properties don't have keyword");
        }
        
        
        return null;
    }

}
