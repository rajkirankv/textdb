package edu.uci.ics.textdb.jsonplangen;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.*;

import edu.uci.ics.textdb.api.dataflow.IOperator;

/**
 * JsonPlanGenerator generates a query plan according to a query model in JSON format.
 * 
 * The JSON query model is currently defined as the following format:
 * 
 * operators: a list of operators.
 * links: a list of links from an operator to another.
 * 
 * each operator contains three attributes:
 * id: an unique ID of this operator
 * operatorType: type of the operator (for example, KeywordMatcher, RegexMatcher)
 * properties: properties that this operators needs (for example, keyword, regex)
 * properties are treated as flat string key-value pairs.
 * 
 * each link contains two attributes:
 * from: operatorID of the link's origin
 * to:   operatorID of the link's destination
 * 
 * Sample query format:
 * {
 *   "operators" : [
 *      {
 *        "id" : "keyword_1",
 *        "operatorType" : "KeywordMatcher",
 *        "properties" : {
 *          "keyword" : "textdb",
 *          "attributeNames" : "title, content",
 *          "atttibuteTypes" : "STRING, TEXT"
 *        }
 *      },
 *      {
 *        "id" : "sink_1",
 *        "operatorType" : "FileSink",
 *        "properties" : {
 *          "path" : "./result.txt"
 *        }
 *      }
 *   ],
 *   
 *   "links" : [
 *      {
 *        "from" : "keyword_1",
 *        "to"   : "sink_1"
 *      }
 *   ]
 * }
 * 
 * @author Zuozhi Wang
 *
 */
public class JsonPlanGenerator {
    
    private static final String OPERATORS = "operators";
    private static final String LINKS = "links";
    
    private static final String OPERATOR_TYPE = "operatorType";
    private static final String OPERATOR_ID = "id";
    private static final String OPERATOR_PROPERTIES = "properties";
    
    // operatorMap stores the operators generated according to their IDs
    private HashMap<String, IOperator> operatorMap;
    
    
    public JsonPlanGenerator() {
        operatorMap = new HashMap<>();    
    }
    
    /**
     * This function generates a query plan according to the input JSON file.
     * @param jsonQueryString
     * @throws Exception
     */
    public void generateQueryPlan(String jsonQueryString) throws Exception {
        JSONObject jsonObject = new JSONObject(jsonQueryString);
        
        JSONArray operatorJsonArray = jsonObject.getJSONArray(OPERATORS);
        
        buildOperators(operatorJsonArray);                
    }
    
    /*
     * The first step of plan generation is to build operators.
     * Operators are defined under "operators" JSON array.
     */
    private void buildOperators(JSONArray operatorJsonArray) throws Exception {
        Iterator<Object> arrayIterator = operatorJsonArray.iterator();
        while (arrayIterator.hasNext()) {
            Object operatorObject = arrayIterator.next();
            assert(operatorObject instanceof JSONObject);
            processOperator((JSONObject) operatorObject);

        }
    }
    
    /*
     * Each operator will have a unique ID (String), operatorType (String), and some operator-specific properties (JSONObject).
     * Properties are a set of string key-value pairs to define some required and optional properties that an operator needs.
     */
    private void processOperator(JSONObject operatorJsonObject) throws Exception {
        String operatorID = operatorJsonObject.getString(OPERATOR_ID);
        String operatorType = operatorJsonObject.getString(OPERATOR_TYPE);
        
        // assert operatorID and operatorType exist
        assert(operatorID != null && ! operatorID.trim().isEmpty());
        assert(operatorType != null && ! operatorType.trim().isEmpty());
        
        // assert operatorID and operatorType are valid
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
