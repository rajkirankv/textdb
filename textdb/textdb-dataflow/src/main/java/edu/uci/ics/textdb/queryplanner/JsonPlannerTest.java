package edu.uci.ics.textdb.queryplanner;


public class JsonPlannerTest {
    
    public static void main(String[] args) {
        new JsonPlanner().generateQueryPlan(SampleJsonQuery.sampleQuery1);
        
    }

}
