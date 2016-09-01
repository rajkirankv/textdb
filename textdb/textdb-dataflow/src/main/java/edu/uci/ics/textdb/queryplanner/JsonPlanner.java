package edu.uci.ics.textdb.queryplanner;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.json.*;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.dataflow.keywordmatch.KeywordMatcher;
import edu.uci.ics.textdb.dataflow.regexmatch.RegexMatcher;

public class JsonPlanner {
    
    private static final String OPERATORS = "operators";
    private static final String LINKS = "links";
    
    private static final String OPERATOR_TYPE = "operatorType";
    private static final String OPERATOR_ID = "id";
    private static final String OPERATOR_PROPERTIES = "properties";
    
    private HashMap<String, IOperator> operatorMap;
    
    
    public JsonPlanner() {
        operatorMap = new HashMap<>();    
    }
    
    
    public void generateQueryPlan(String jsonQueryString) throws Exception {
        JSONObject jsonObject = new JSONObject(jsonQueryString);
        
        JSONArray operatorJsonArray = jsonObject.getJSONArray(OPERATORS);
        JSONArray linksJsonArray = jsonObject.getJSONArray(LINKS);
        
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
        assert(JsonPlannerConstants.isValidOperator(operatorType));
        assert(! operatorMap.keySet().contains(operatorID));
        
        JSONObject operatorPropertiesObject = operatorJsonObject.getJSONObject(OPERATOR_PROPERTIES);        
        Map<String, String> operatorProperties = new HashMap<>();
        
        if (operatorPropertiesObject != null) {
            for (String key : operatorPropertiesObject.keySet()) {
                operatorProperties.put(key, operatorPropertiesObject.get(key).toString());
            }
        }
              
        IOperator operator = JsonPlannerConstants.buildOperator(operatorType, operatorID, operatorProperties);
        operatorMap.put(operatorID, operator);
        
        if (operator instanceof KeywordMatcher) {
            KeywordMatcher keywordMatcher = (KeywordMatcher) operator;
            System.out.println("keyword matcher successfully built!");
            System.out.println("keyword:   "+keywordMatcher.getPredicate().getQuery());
            System.out.println("attrList:  "+keywordMatcher.getPredicate().getAttributeList());
            System.out.println("matchType: "+keywordMatcher.getPredicate().getOperatorType());
            System.out.println("limit:     "+keywordMatcher.getLimit());
            System.out.println("offset:    "+keywordMatcher.getOffset());
            System.out.println();
        }
        
        if (operator instanceof RegexMatcher) {
            RegexMatcher regexMatcher = (RegexMatcher) operator;
            System.out.println("regex matcher successfully built!");
            System.out.println("regex:    "+regexMatcher.getPredicate().getRegex());
            System.out.println("attrList: "+regexMatcher.getPredicate().getAttributeList());
            System.out.println("limit:    "+regexMatcher.getLimit());
            System.out.println("offset:   "+regexMatcher.getOffset());
            System.out.println();
        }
        
    }
    
    

}
