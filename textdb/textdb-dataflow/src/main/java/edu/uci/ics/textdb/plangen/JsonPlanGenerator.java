package edu.uci.ics.textdb.plangen;

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
 *   id: a unique ID of this operator
 *   operatorType: type of the operator (e.g., KeywordMatcher, RegexMatcher)
 *   properties: properties needed by this operator (e.g., keyword, regex)
 *              properties are treated as unnested key-value string pairs.
 * 
 * each link contains two attributes:
 *   from: operatorID of the link's origin
 *   to:   operatorID of the link's destination
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
    
    private static final String FROM = "from";
    private static final String TO = "to";
    
    // operatorMap stores the operators generated according to their IDs
    private HashMap<String, IOperator> operatorMap;
    private HashMap<String, String> operatorTypeMap;
    private OperatorDAG<String> operatorDAG;
    
    
    public JsonPlanGenerator() {
        operatorMap = new HashMap<>();    
        operatorTypeMap = new HashMap<>();
        operatorDAG = new OperatorDAG();
    }
    
    /**
     * This function generates a query plan according to the input JSON file.
     * @param jsonQueryString
     * @throws Exception
     */
    public void generateQueryPlan(String jsonQueryString) throws Exception {
        JSONObject jsonObject = new JSONObject(jsonQueryString);
        
        JSONArray operatorJsonArray = jsonObject.getJSONArray(OPERATORS);
        JSONArray linkJsonArray = jsonObject.getJSONArray(LINKS);
        
        buildOperators(operatorJsonArray);
        buildLinks(linkJsonArray);
    }
    
    /*
     * The first step of plan generation is to build operators.
     * Operators are defined under "operators" JSON array.
     */
    private void buildOperators(JSONArray operatorJsonArray) throws Exception {
        Iterator<Object> arrayIterator = operatorJsonArray.iterator();
        while (arrayIterator.hasNext()) {
            Object operatorObject = arrayIterator.next();
            PlanGenUtils.planGenAssert(operatorObject instanceof JSONObject, "invalid JSON format");
            processOperator((JSONObject) operatorObject);

        }
    }
    
    /*
     * Each operator has a unique ID (String), operatorType (String), 
     * and some operator-specific properties (JSONObject).
     * Properties are a set of key-value string pairs to define 
     * some required and optional properties needed by an operator.
     */
    private void processOperator(JSONObject operatorJsonObject) throws Exception {
        String operatorID = operatorJsonObject.getString(OPERATOR_ID);
        String operatorType = operatorJsonObject.getString(OPERATOR_TYPE);
        
        // assert operatorID and operatorType exist
        PlanGenUtils.planGenAssert(operatorID != null, "operatorID doesn't exist");
        PlanGenUtils.planGenAssert(! operatorID.trim().isEmpty(), "operatorID is empty");
        PlanGenUtils.planGenAssert(operatorType != null, "operatorType doesn't exist");
        PlanGenUtils.planGenAssert(!  operatorType.trim().isEmpty(), "operatorType is empty");
        
        // assert operatorID and operatorType are valid
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidOperator(operatorType), "operatorType is not valid");
        // TODO: change ID to case insensitive
        PlanGenUtils.planGenAssert(! operatorMap.keySet().contains(operatorID), "duplicate operatorID, each ID must be unique");
        
        JSONObject operatorPropertiesObject = operatorJsonObject.getJSONObject(OPERATOR_PROPERTIES);        
        Map<String, String> operatorProperties = new HashMap<>();
        
        if (operatorPropertiesObject != null) {
            for (String key : operatorPropertiesObject.keySet()) {
                operatorProperties.put(key, operatorPropertiesObject.get(key).toString());
            }
        }
              
        IOperator operator = PlanGenUtils.buildOperator(operatorType, operatorProperties);
        operatorMap.put(operatorID, operator);   
        operatorTypeMap.put(operatorID,operatorType);
        operatorDAG.addVertex(operatorID);
    }
    
    private void buildLinks(JSONArray linkJSONArray) throws Exception {
        Iterator<Object> arrayIterator = linkJSONArray.iterator();
        while (arrayIterator.hasNext()) {
            Object linkObject = arrayIterator.next();
            PlanGenUtils.planGenAssert(linkObject instanceof JSONObject, "invalid JSON format");
            JSONObject linkJsonObject = (JSONObject) linkObject;
            
            String fromString = linkJsonObject.getString(FROM);
            String toString = linkJsonObject.getString(TO);
            
            PlanGenUtils.planGenAssert(fromString != null, "from property doesn't exist");
            PlanGenUtils.planGenAssert(! fromString.trim().isEmpty(), "from property is empty");
            PlanGenUtils.planGenAssert(toString != null, "to property doesn't exist");
            PlanGenUtils.planGenAssert(!  toString.trim().isEmpty(), "to property is empty");
            
            operatorDAG.addEdge(fromString, toString);
        }
    }

    
    
    public Map<String, IOperator> getOperatorMap() {
        return operatorMap;
    }

}
