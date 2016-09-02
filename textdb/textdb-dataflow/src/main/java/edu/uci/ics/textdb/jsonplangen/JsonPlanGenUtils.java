package edu.uci.ics.textdb.jsonplangen;

import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.PlanGenException;

/**
 * This class provides a set of helper functions that are commonly used in generating plan.
 * @author Zuozhi Wang
 *
 */
public class JsonPlanGenUtils {
    
    public static void planGenAssert(boolean assertBoolean) throws PlanGenException {
        planGenAssert(assertBoolean, "");
    }
    
    public static void planGenAssert(boolean assertBoolean, String errorMessage) throws PlanGenException {
        if (! assertBoolean) {
            throw new PlanGenException(errorMessage);
        }
    }
    
    /**
     * This function checks if a string is a valid operator (case insensitive).
     * 
     * @param operatorStr
     * @return true if the string is an operator
     */
    public static boolean isValidOperator(String operatorStr) {
        return JsonPlanGenConstants.operatorList.stream().anyMatch(str -> str.toLowerCase().equals(operatorStr.toLowerCase()));
    }

    /**
     * This function checks if a string is a valid attribute type (case
     * insensitive).
     * 
     * @param attributeType
     * @return true if the string is an attribute type
     */
    public static boolean isValidAttributeType(String attributeType) {
        return JsonPlanGenConstants.attributeTypeList.stream().anyMatch(str -> str.toLowerCase().equals(attributeType.toLowerCase()));
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

        Class<? extends OperatorBuilder> operatorBuilderClass = JsonPlanGenConstants.operatorBuilderMap.get(operatorType.toLowerCase());
        OperatorBuilder operatorBuilder = operatorBuilderClass
                .getConstructor(String.class, Map.class)
                .newInstance(operatorID, operatorProperties);
        
        return operatorBuilder.build();
    }

}
