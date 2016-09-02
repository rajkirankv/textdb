package edu.uci.ics.textdb.jsonplangen;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.exception.PlanGenException;

/**
 * OperatorBuilder is a base abstract class for building an operator based on
 * its properties. Sub classes needs to implement the build() function, which
 * builds the IOperator object and returns it.
 * 
 * This abstract class also provides some helper functions that will be commonly
 * used when building operators.
 * 
 * This abstract class also defines some commonly used variables: operatorID and
 * operatorProperties: these two variables are available for sub-classes to
 * directly use.
 * 
 * ATTRIBUTE_NAMES, ATTRIBUTE_TYPES, LIMIT, OFFSET: commonly used keys in
 * operatorProperties.
 * 
 * @author Zuozhi Wang (zuozhiw)
 *
 */
public abstract class OperatorBuilder {

    protected String operatorID;
    protected Map<String, String> operatorProperties;

    public static final String ATTRIBUTE_NAMES = "attributeNames";
    public static final String ATTRIBUTE_TYPES = "attributeTypes";
    public static final String LIMIT = "limit";
    public static final String OFFSET = "offset";

    /**
     * Construct an OperatorBulider with operatorID and operatorProperties
     * 
     * @param operatorID
     * @param operatorProperties
     */
    public OperatorBuilder(String operatorID, Map<String, String> operatorProperties) {
        this.operatorID = operatorID;
        this.operatorProperties = operatorProperties;
    }

    public abstract IOperator build() throws Exception;

    /**
     * This function returns a property that is required. An exception is thrown
     * if the operator properties doens't contain the key.
     * 
     * @param key
     * @return value
     * @throws PlanGenException,
     *             if the operator properties doens't contain the key.
     */
    protected String getRequiredProperty(String key) throws PlanGenException {
        if (operatorProperties.containsKey(key)) {
            return operatorProperties.get(key);
        } else {
            throw new PlanGenException(operatorID + " missing required key " + key);
        }
    }

    /**
     * This function returns a property that is optional. Null will be returned
     * if the operator properties doens't contain the key.
     * 
     * @param key
     * @return value, null if the operator properties doens't contain the key.
     */
    protected String getOptionalProperty(String key) {
        return operatorProperties.get(key);
    }

    /**
     * This function finds properties related to constructing the attributes in
     * operatorProperties, and converts them to a list of attributes.
     * 
     * @return a list of attributes
     * @throws PlanGenException
     */
    protected List<Attribute> constructAttributeList() throws PlanGenException {
        String attributeNamesStr = getRequiredProperty(ATTRIBUTE_NAMES);
        String attributeTypesStr = getRequiredProperty(ATTRIBUTE_TYPES);

        List<String> attributeNames = splitAttributes(attributeNamesStr);
        List<String> attributeTypes = splitAttributes(attributeTypesStr);

        assert (attributeNames.size() == attributeTypes.size());
        assert (attributeTypes.stream().allMatch(typeStr -> JsonPlanGenConstants.isValidAttributeType(typeStr)));

        List<Attribute> attributeList = IntStream.range(0, attributeNames.size()) // for each index in the list
                .mapToObj(i -> constructAttribute(attributeNames.get(i), attributeTypes.get(i))) // construct an attribute
                .collect(Collectors.toList());

        return attributeList;
    }

    private Attribute constructAttribute(String attributeNameStr, String attributeTypeStr) {
        FieldType fieldType = FieldType.valueOf(attributeTypeStr.toUpperCase());
        return new Attribute(attributeNameStr, fieldType);
    }

    private List<String> splitAttributes(String attributesStr) {
        String[] attributeArray = attributesStr.split(",");
        return Arrays.asList(attributeArray).stream().map(s -> s.trim()).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }

    /**
     * This function finds limit in operator properties, return null if not
     * found.
     * 
     * @return limit, null if not found
     * @throws PlanGenException
     */
    protected Integer findLimit() throws PlanGenException {
        String limitStr = getOptionalProperty(LIMIT);
        if (limitStr == null) {
            return null;
        }
        Integer limit = Integer.parseInt(limitStr);
        if (limit < 0) {
            throw new PlanGenException("Limit must be equal to or greater than 0");
        }
        return limit;
    }

    /**
     * This function finds offset in operator properties, return null if not
     * found.
     * 
     * @return offset, null if not found
     * @throws PlanGenException
     */
    protected Integer findOffset() throws PlanGenException {
        String offsetStr = getOptionalProperty(OFFSET);
        if (offsetStr == null) {
            return null;
        }
        Integer offset = Integer.parseInt(offsetStr);
        if (offset < 0) {
            throw new PlanGenException("Offset must be equal to or greater than 0");
        }
        return offset;
    }

}
