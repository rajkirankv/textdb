package edu.uci.ics.textdb.plangen;

import java.util.HashMap;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.JoinBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.NlpExtractorBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;

public class OperatorGraphTest {
    
    
    public static HashMap<String, String> keywordSourceProperties1 = new HashMap<String, String>() {{
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
        
        put(KeywordMatcherBuilder.KEYWORD, "irvine");
        put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
        put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
        put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
        put(OperatorBuilderUtils.DATA_DIRECTORY, "./index");
        put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
    }};
    
    public static HashMap<String, String> regexMatcherProperties1 = new HashMap<String, String>() {{
        put(RegexMatcherBuilder.REGEX, "ca(lifornia)?");
        put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "location, content");
        put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "string, text");
    }};
    
    public static HashMap<String, String> nlpExtractorProperties1 = new HashMap<String, String>() {{
        put(NlpExtractorBuilder.NLP_TYPE, "date");
        put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
    }};
    
    public static HashMap<String, String> joinProperties1 = new HashMap<String, String>() {{
        put(JoinBuilder.JOIN_PREDICATE, "CharacterDistance");
        put(JoinBuilder.JOIN_DISTANCE, "100");
        put(JoinBuilder.JOIN_ID_ATTRIBUTE_NAME, "id");
        put(JoinBuilder.JOIN_ID_ATTRIBUTE_TYPE, "integer");
        put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "content");
        put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "text");
    }};
    
    public static HashMap<String, String> fileSinkProperties1 = new HashMap<String, String>() {{
        put(FileSinkBuilder.FILE_PATH, "./result.txt");
    }};
    

    @Test
    public void OperatorGraphTest1() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();
   
        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);       
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);        
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties1);      
        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("regex", "sink");
                
        Plan queryPlan = operatorGraph.buildQueryPlan();  
        ISink sink = queryPlan.getRoot();
    }
    
    
    @Test
    public void OperatorGraphTest2() throws Exception {
        OperatorGraph operatorGraph = new OperatorGraph();
        
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
        
        operatorGraph.addOperator("source", "KeywordSource", keywordSourceProperties1);   
        operatorGraph.addOperator("regex", "RegexMatcher", regexMatcherProperties1);     
        operatorGraph.addOperator("nlp", "NlpExtractor", nlpExtractorProperties1);       
        operatorGraph.addOperator("join", "Join", joinProperties1);       
        operatorGraph.addOperator("sink", "FileSink", fileSinkProperties1);
        
        operatorGraph.addLink("source", "regex");
        operatorGraph.addLink("source", "nlp");
        operatorGraph.addLink("regex", "join");
        operatorGraph.addLink("nlp", "join");
        operatorGraph.addLink("join", "sink");
                
        Plan queryPlan = operatorGraph.buildQueryPlan();        
        ISink sink = queryPlan.getRoot();
    }

}
