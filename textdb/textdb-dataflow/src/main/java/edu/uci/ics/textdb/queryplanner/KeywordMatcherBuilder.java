package edu.uci.ics.textdb.queryplanner;

import java.util.List;
import java.util.stream.Stream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.constants.DataConstants.KeywordMatchingType;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.KeywordPredicate;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;

public class KeywordMatcherBuilder extends OperatorBuilder {
    
    private static final String KEYWORD = "keyword";
    private static final String ATTRIBUTE_NAMES = "attributeNames";
    private static final String ATTRIBUTE_TYPES = "attributeTypes";
    private static final String MATCHING_TYPE = "matchingType";
    private static final String LIMIT = "limit";
    private static final String OFFSET = "offset";
    
    
    public KeywordMatcher build() throws ParseException, DataFlowException {
        String keyword = getRequiredProperty(KEYWORD);
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES);
        String attributeTypesStr = getRequiredProperty(ATTRIBUTE_TYPES);
        String matchingTypeStr = getRequiredProperty(MATCHING_TYPE);
        
        String limitStr = getOptionalProperty(LIMIT);
        String offsetStr = getOptionalProperty(OFFSET);
        
        // check if keyword is empty
        assert(! keyword.trim().isEmpty());
        
        // generate attribute list 
        List<Attribute> attributeList = constructAttributeList(attributeNamesStr, attributeTypesStr);
        
        // generate matching type
        assert(isValidKeywordMatchingType(matchingTypeStr));
        KeywordMatchingType matchingType = KeywordMatchingType.valueOf(matchingTypeStr.toUpperCase());
        
        // build KeywordMatcher
        KeywordPredicate keywordPredicate = new KeywordPredicate(keyword, attributeList, DataConstants.getStandardAnalyzer(), matchingType);
        KeywordMatcher keywordMatcher = new KeywordMatcher(keywordPredicate);
        
        // set limit and offset
        Integer limitInt = tryParseInt(limitStr);
        if (limitInt != null && limitInt >= 0) {
            keywordMatcher.setLimit(limitInt);
        }
        Integer offsetInt = tryParseInt(offsetStr);
        if (offsetInt != null && offsetInt >= 0) {
            keywordMatcher.setOffset(offsetInt);
        }

        return keywordMatcher;
    }
    
    private Integer tryParseInt(String intStr) {
        if (intStr == null || intStr.trim().isEmpty()) {
            return null;
        }
        try {
            return Integer.parseInt(intStr);
        } catch (NumberFormatException e) {
            return null;
        }
    }
    
    private boolean isValidKeywordMatchingType(String matchingTypeStr) {
        return Stream.of(KeywordMatchingType.values())
                .map(KeywordMatchingType::name)
                .anyMatch(name -> name.toUpperCase().equals(matchingTypeStr.toUpperCase()));        
    }
    

}
