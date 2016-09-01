package edu.uci.ics.textdb.queryplanner;

import java.io.IOException;
import java.util.List;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.DataConstants;
import edu.uci.ics.textdb.common.exception.DataFlowException;
import edu.uci.ics.textdb.dataflow.common.RegexPredicate;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;

public class RegexMatcherBuilder extends OperatorBuilder {
    
    private static final String REGEX = "regex"; 
    
    @Override
    public IOperator build() throws ParseException, DataFlowException, IOException {
        String regex = getRequiredProperty(REGEX);
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES);
        String attributeTypesStr = getRequiredProperty(ATTRIBUTE_TYPES);
        
        String limitStr = getOptionalProperty(LIMIT);
        String offsetStr = getOptionalProperty(OFFSET);
        
        // check if regex is empty
        assert(! regex.trim().isEmpty());
        
        // generate attribute list 
        List<Attribute> attributeList = constructAttributeList(attributeNamesStr, attributeTypesStr);

        // build KeywordMatcher
        RegexPredicate regexPredicate = new RegexPredicate(regex, attributeList, DataConstants.getTrigramAnalyzer());
        RegexMatcher regexMatcher = new RegexMatcher(regexPredicate);
        
        // set limit and offset
        Integer limitInt = tryParseInt(limitStr);
        if (limitInt != null && limitInt >= 0) {
            regexMatcher.setLimit(limitInt);
        }
        Integer offsetInt = tryParseInt(offsetStr);
        if (offsetInt != null && offsetInt >= 0) {
            regexMatcher.setOffset(offsetInt);
        }

        return regexMatcher;
    }

}
