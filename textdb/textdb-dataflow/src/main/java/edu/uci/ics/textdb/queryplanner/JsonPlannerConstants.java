package edu.uci.ics.textdb.queryplanner;

import java.util.Arrays;
import java.util.List;

public class JsonPlannerConstants {
    
    public static List<String> operatorList = Arrays.asList(
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
    
    public static boolean isValidOperator(String operatorStr) {
        return operatorList.stream().map(str -> str.toLowerCase()).anyMatch(str -> str.equals(operatorStr));
    }
    
    
    
//    public static HashMap<String, >

}
