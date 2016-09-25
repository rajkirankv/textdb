package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.Map;

public class OperatorArityConstants {
    
    public static Map<String, Integer> fixedInputArityMap = new HashMap<>();
    static {
        fixedInputArityMap.put("KeywordSource".toLowerCase(), 0);
        fixedInputArityMap.put("DictionarySource".toLowerCase(), 0);
        
        fixedInputArityMap.put("KeywordMatcher".toLowerCase(), 1);
        fixedInputArityMap.put("DictionaryMatcher".toLowerCase(), 1);
        fixedInputArityMap.put("RegexMatcher".toLowerCase(), 1);
        fixedInputArityMap.put("NlpExtractor".toLowerCase(), 1);
        fixedInputArityMap.put("FuzzyTokenMatcher".toLowerCase(), 1);
        
        fixedInputArityMap.put("FileSink".toLowerCase(), 1);
        fixedInputArityMap.put("IndexSink".toLowerCase(), 1);
      
        fixedInputArityMap.put("Join".toLowerCase(), 2);
    }
    
    public static Map<String, Integer> fixedOutputArityMap = new HashMap<>();
    static {
        fixedOutputArityMap.put("IndexSink".toLowerCase(), 0);
        fixedOutputArityMap.put("FileSink".toLowerCase(), 0);
        
        fixedOutputArityMap.put("KeywordMatcher".toLowerCase(), 1);
        fixedOutputArityMap.put("DictionaryMatcher".toLowerCase(), 1);
        fixedOutputArityMap.put("RegexMatcher".toLowerCase(), 1);
        fixedOutputArityMap.put("NlpExtractor".toLowerCase(), 1);
        fixedOutputArityMap.put("FuzzyTokenMatcher".toLowerCase(), 1);
        
        fixedOutputArityMap.put("KeywordSource".toLowerCase(), 1);
        fixedOutputArityMap.put("DictionarySource".toLowerCase(), 1);
        
        fixedOutputArityMap.put("Join".toLowerCase(), 1);
    }
    
    
    public static boolean checkInputArity(String operatorType, int actualInputArity) {
        if (fixedInputArityMap.containsKey(operatorType.toLowerCase())) {
            return fixedInputArityMap.get(operatorType.toLowerCase()) == actualInputArity;
        } else {
            return false;
        }
    }
    
    public static boolean checkOutputArity(String operatorType, int actualOutputArity) {
        if (fixedOutputArityMap.containsKey(operatorType.toLowerCase())) {
            return fixedOutputArityMap.get(operatorType.toLowerCase()) == actualOutputArity;
        } else {
            return false;
        }
    }
    
}
