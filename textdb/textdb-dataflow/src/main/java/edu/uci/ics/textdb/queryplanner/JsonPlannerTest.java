package edu.uci.ics.textdb.queryplanner;


public class JsonPlannerTest {
    
    public static void main(String[] args) throws Exception {
        new JsonPlanner().generateQueryPlan(SampleJsonQuery.sampleJsonQueryRegexMatcher);
        
        
    }

}
