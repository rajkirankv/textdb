package edu.uci.ics.textdb.plangen;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.json.JSONObject;
import org.junit.Test;

import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.plangen.operatorbuilder.FileSinkBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.KeywordMatcherBuilder;
import edu.uci.ics.textdb.plangen.operatorbuilder.OperatorBuilderUtils;
import edu.uci.ics.textdb.plangen.operatorbuilder.RegexMatcherBuilder;

public class OperatorGraphTest {
    
    @Test
    public void OperatorGraphTest1() throws PlanGenException {
        OperatorGraph operatorGraph = new OperatorGraph();
        
        JSONObject schemaJsonJSONObject = new JSONObject();
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "id, city, location, content");
        schemaJsonJSONObject.put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "integer, string, string, text");
        
        operatorGraph.addOperator("source", "KeywordSource", 
                new HashMap<String, String>() {{
                    put(KeywordMatcherBuilder.KEYWORD, "irvine");
                    put(KeywordMatcherBuilder.MATCHING_TYPE, "PHRASE_INDEXBASED");
                    put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "city, location, content");
                    put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "STRING, STRING, TEXT");
                    put(OperatorBuilderUtils.DATA_DIRECTORY, "./index");
                    put(OperatorBuilderUtils.SCHEMA, schemaJsonJSONObject.toString());
                }});
        
        operatorGraph.addOperator("regex", "RegexMatcher", 
                new HashMap<String, String>() {{
                    put(RegexMatcherBuilder.REGEX, "ca(lifornia)?");
                    put(OperatorBuilderUtils.ATTRIBUTE_NAMES, "location, content");
                    put(OperatorBuilderUtils.ATTRIBUTE_TYPES, "string, text");
                }});
        
        operatorGraph.addOperator("sink", "FileSink", 
                new HashMap<String, String>() {{
                    put(FileSinkBuilder.FILE_PATH, "./result.txt");
                }});
        
        
    }

}
