package edu.uci.ics.textdb.jsonplangen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This class provides a set of constants that would be used in JsonPlanGenerator.
 * 
 * @author Zuozhi Wang
 *
 */
public class JsonPlanGenConstants {
    
    // This class doesn't need to be initialized.
    private JsonPlanGenConstants() {
    }

    /**
     * A list of all operators of TextDB.
     */
    public static final List<String> operatorList = Arrays.asList(
            "IndexBasedSource", 
            "ScanBasedSource",

            "KeywordMatcher", 
            "RegexMatcher", 
            "FuzzyTokenMatcher", 
            "NlpExtractor",

            "Join",

            "IndexSink", 
            "FileSink");

    /**
     * A list of all attribute types (field types) of TextDB.
     */
    public static final List<String> attributeTypeList = Arrays.asList(
            "Integer", 
            "Double", 
            "Date", 
            "String", 
            "Text");

    /**
     * A map of operators to the their builder classes.
     */
    public static final Map<String, Class<? extends OperatorBuilder>> operatorBuilderMap;
    static {
        operatorBuilderMap = new HashMap<>();
        operatorBuilderMap.put("KeywordMatcher".toLowerCase(), KeywordMatcherBuilder.class);
    }

}
