package edu.uci.ics.textdb.queryplanner;

import java.util.HashMap;
import java.util.Iterator;

import org.json.*;

public class JsonPlanner {
    
    private static final String OPERATORS = "operators";
    private static final String LINKS = "links";
    
    private static final String OPERATOR_TYPE = "operatorType";
    private static final String OPERATOR_ID = "id";
    
    private HashMap<String, Object> operatorMap;
    
    
    public JsonPlanner() {
        operatorMap = new HashMap<>();    
    }
    
    
    public void generateQueryPlan(String jsonQueryString) {
        JSONObject jsonObject = new JSONObject(jsonQueryString);
        
        JSONArray operatorJsonArray = jsonObject.getJSONArray(OPERATORS);
        JSONArray linksJsonArray = jsonObject.getJSONArray(LINKS);
        
        buildOperators(operatorJsonArray);
        
        
    }
    
    private void buildOperators(JSONArray operatorJsonArray) {
        Iterator<Object> arrayIterator = operatorJsonArray.iterator();
        while (arrayIterator.hasNext()) {
            Object operatorObject = arrayIterator.next();
            assert(operatorObject instanceof JSONObject);
            processOperator((JSONObject) operatorObject);

        }
    }
    
    private void processOperator(JSONObject operatorJsonObject) {
        String operatorID = operatorJsonObject.getString(OPERATOR_ID);
        String operatorType = operatorJsonObject.getString(OPERATOR_TYPE);
        
        // case sensitive or not ?
        assert(JsonPlannerConstants.isValidOperator(operatorType));
        assert(! operatorMap.keySet().contains(operatorID));
              
        for (String key : operatorJsonObject.keySet()) {
            System.out.println("key: "+key);
            System.out.println("values: "+operatorJsonObject.get(key));
        }
    }
    
    

}
