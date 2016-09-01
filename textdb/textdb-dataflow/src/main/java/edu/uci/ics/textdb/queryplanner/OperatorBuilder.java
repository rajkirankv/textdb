package edu.uci.ics.textdb.queryplanner;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import edu.uci.ics.textdb.api.common.Attribute;
import edu.uci.ics.textdb.api.common.FieldType;
import edu.uci.ics.textdb.api.dataflow.IOperator;

public abstract class OperatorBuilder {
    
    protected String operatorID;
    protected Map<String, String> operatorProperties;
    
    public OperatorBuilder() {
    }
    
    public void specifyIDAndProperties(String operatorID, Map<String, String> operatorProperties) {
        this.operatorID = operatorID;
        this.operatorProperties = operatorProperties;
    }
    
    public abstract IOperator build() throws Exception;
    
    protected String getRequiredProperty(String key) throws ParseException {
        if (operatorProperties.containsKey(key)) {
            return operatorProperties.get(key);
        } else {
            throw new ParseException(operatorID+" missing required key "+key);
        }
    }
    
    protected String getOptionalProperty(String key) {
        return operatorProperties.get(key);
    }
    
    protected List<Attribute> constructAttributeList(String attributeNamesStr, String attributeTypesStr) {
        List<String> attributeNames = splitAttributes(attributeNamesStr);
        List<String> attributeTypes = splitAttributes(attributeTypesStr);
        
        assert(attributeNames.size() == attributeTypes.size());
        assert(attributeTypes.stream().allMatch(typeStr -> JsonPlannerConstants.isValidAttributeType(typeStr)));
        
        List<Attribute> attributeList = IntStream.range(0, attributeNames.size())               // for each index in the list
                .mapToObj(i -> constructAttribute(attributeNames.get(i), attributeTypes.get(i)))// construct an attribute
                .collect(Collectors.toList());
        
        return attributeList;
    }
    
    private Attribute constructAttribute(String attributeNameStr, String attributeTypeStr) {
        FieldType fieldType = FieldType.valueOf(attributeTypeStr.toUpperCase());
        return new Attribute(attributeNameStr, fieldType);
    }
      
    private List<String> splitAttributes(String attributesStr) {
        String[] attributeArray = attributesStr.split(",");
        return Arrays.asList(attributeArray).stream().map(s -> s.trim()).filter(s -> !s.isEmpty()).collect(Collectors.toList());
    }

}
