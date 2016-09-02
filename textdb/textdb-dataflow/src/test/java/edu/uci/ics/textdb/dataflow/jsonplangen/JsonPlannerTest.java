package edu.uci.ics.textdb.dataflow.jsonplangen;

import edu.uci.ics.textdb.jsonplangen.JsonPlanGenerator;

public class JsonPlannerTest {
    
    public static void main(String[] args) throws Exception {
        new JsonPlanGenerator().generateQueryPlan(SampleJsonQuery.sampleJsonQueryRegexMatcher);
        
        
    }

}
