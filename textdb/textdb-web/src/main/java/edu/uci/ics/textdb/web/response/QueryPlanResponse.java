package edu.uci.ics.textdb.web.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;

import java.util.ArrayList;

/**
 * @author Kishore Narendran on 2/16/17.
 */
public class QueryPlanResponse {

    @JsonProperty("query_plans")
    private ArrayList<QueryPlanRequest> queryPlans;

    public QueryPlanResponse() {
    }

    public QueryPlanResponse(ArrayList<QueryPlanRequest> queryPlans) {
        this.queryPlans = queryPlans;
    }

    @JsonProperty("query_plans")
    public ArrayList<QueryPlanRequest> getQueryPlans() {
        return queryPlans;
    }

    @JsonProperty("query_plans")
    public void setQueryPlans(ArrayList<QueryPlanRequest> queryPlans) {
        this.queryPlans = queryPlans;
    }
}
