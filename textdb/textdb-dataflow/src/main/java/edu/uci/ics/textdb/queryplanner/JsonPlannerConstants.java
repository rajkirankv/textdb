package edu.uci.ics.textdb.queryplanner;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;

public class JsonPlannerConstants {
    
    public static final List<String> operatorList = Arrays.asList(
            "IndexBasedSource",
            "ScanBasedSource",
            
            "KeywordMatcher",
            "RegexMatcher",
            "FuzzyTokenMatcher",
            "NlpExtractor",
            
            "Join",
            
            "IndexSink",
            "FileSink"
            );
    
    public static final List<String> attributeTypeList = Arrays.asList(
            "Integer",
            "Double",
            "Date",
            "String",
            "Text"
            );
    
    public static final Map<String, Class<? extends OperatorBuilder>> operatorBuilderMap;
    static {
        operatorBuilderMap = new HashMap<>();
        operatorBuilderMap.put("KeywordMatcher".toLowerCase(), KeywordMatcherBuilder.class);
        operatorBuilderMap.put("RegexMatcher".toLowerCase(), RegexMatcherBuilder.class);
    }
    
    public static boolean isValidOperator(String operatorStr) {
        return operatorList.stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }
    
    public static boolean isValidAttributeType(String attributeType) {
        return attributeTypeList.stream().anyMatch(str -> str.toLowerCase().equals(attributeType.toLowerCase()));
    }
    
    public static IOperator buildOperator(String operatorType, String operatorID, Map<String, String> operatorProperties) throws Exception {
        OperatorBuilder operatorBuilder = operatorBuilderMap.get(operatorType.toLowerCase()).newInstance();
        operatorBuilder.specifyIDAndProperties(operatorID, operatorProperties);
        return operatorBuilder.build();
    }
    
    
    
    
//    public static HashMap<String, >

}
