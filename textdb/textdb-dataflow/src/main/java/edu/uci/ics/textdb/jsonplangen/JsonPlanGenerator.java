package edu.uci.ics.textdb.jsonplangen;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.*;

import edu.uci.ics.textdb.api.dataflow.IOperator;

public class JsonPlanGenerator {
    
    private static final String OPERATORS = "operators";
    private static final String LINKS = "links";
    
    private static final String OPERATOR_TYPE = "operatorType";
    private static final String OPERATOR_ID = "id";
    private static final String OPERATOR_PROPERTIES = "properties";
    
    private HashMap<String, IOperator> operatorMap;
    
    
    public JsonPlanGenerator() {
        operatorMap = new HashMap<>();    
    }
    
    
    public void generateQueryPlan(String jsonQueryString) throws Exception {
        JSONObject jsonObject = new JSONObject(jsonQueryString);
        
        JSONArray operatorJsonArray = jsonObject.getJSONArray(OPERATORS);
        
        buildOperators(operatorJsonArray);                
    }
    
    private void buildOperators(JSONArray operatorJsonArray) throws Exception {
        Iterator<Object> arrayIterator = operatorJsonArray.iterator();
        while (arrayIterator.hasNext()) {
            Object operatorObject = arrayIterator.next();
            assert(operatorObject instanceof JSONObject);
            processOperator((JSONObject) operatorObject);

        }
    }
    
    private void processOperator(JSONObject operatorJsonObject) throws Exception {
        String operatorID = operatorJsonObject.getString(OPERATOR_ID);
        String operatorType = operatorJsonObject.getString(OPERATOR_TYPE);
        
        // assert operatorID and operatorType exist
        assert(operatorID != null && ! operatorID.trim().isEmpty());
        assert(operatorType != null && ! operatorType.trim().isEmpty());
        
        // assert operatorID and operatorType are valid
        // TODO: case sensitive or not ?
        assert(JsonPlanGenConstants.isValidOperator(operatorType));
        assert(! operatorMap.keySet().contains(operatorID));
        
        JSONObject operatorPropertiesObject = operatorJsonObject.getJSONObject(OPERATOR_PROPERTIES);        
        Map<String, String> operatorProperties = new HashMap<>();
        
        if (operatorPropertiesObject != null) {
            for (String key : operatorPropertiesObject.keySet()) {
                operatorProperties.put(key, operatorPropertiesObject.get(key).toString());
            }
        }
              
        IOperator operator = JsonPlanGenConstants.buildOperator(operatorType, operatorID, operatorProperties);
        operatorMap.put(operatorID, operator);

        
    }
    
    
    public Map<String, IOperator> getOperatorMap() {
        return operatorMap;
    }

}
