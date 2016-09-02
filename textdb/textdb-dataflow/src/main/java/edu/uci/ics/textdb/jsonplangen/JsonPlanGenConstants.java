package edu.uci.ics.textdb.jsonplangen;

import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * 
 * @author Zuozhi Wang (zuozhiw)
 *
 */
public class JsonPlanGenConstants {

    /**
     * A list of all operators of TextDB.
     */
    public static final List<String> operatorList = Arrays.asList("IndexBasedSource", "ScanBasedSource",

            "KeywordMatcher", "RegexMatcher", "FuzzyTokenMatcher", "NlpExtractor",

            "Join",

            "IndexSink", "FileSink");

    /**
     * A list of all attribute types (field types) of TextDB.
     */
    public static final List<String> attributeTypeList = Arrays.asList("Integer", "Double", "Date", "String", "Text");

    /**
     * A map of operators to the their builder classes.
     */
    public static final Map<String, Class<? extends OperatorBuilder>> operatorBuilderMap;
    static {
        operatorBuilderMap = new HashMap<>();
        operatorBuilderMap.put("KeywordMatcher".toLowerCase(), KeywordMatcherBuilder.class);
    }

    /**
     * This function checks if a string is a valid operator (case insensitive).
     * 
     * @param operatorStr
     * @return true if the string is an operator
     */
    public static boolean isValidOperator(String operatorStr) {
        return operatorList.stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }

    /**
     * This function checks if a string is a valid attribute type (case
     * insensitive).
     * 
     * @param attributeType
     * @return true if the string is an attribute type
     */
    public static boolean isValidAttributeType(String attributeType) {
        return attributeTypeList.stream().anyMatch(str -> str.toLowerCase().equals(attributeType.toLowerCase()));
    }

    /**
     * This function builds the operator based on the type, id and properties.
     * 
     * @param operatorType
     * @param operatorID
     * @param operatorProperties
     * @return operator that is built
     * @throws Exception
     */
    public static IOperator buildOperator(String operatorType, String operatorID,
            Map<String, String> operatorProperties) throws Exception {
        Constructor<? extends OperatorBuilder> operatorBuilderConstructor = operatorBuilderMap
                .get(operatorType.toLowerCase()).getConstructor(String.class, Map.class);
        OperatorBuilder operatorBuilder = operatorBuilderConstructor.newInstance(operatorID, operatorProperties);
        return operatorBuilder.build();
    }

}
