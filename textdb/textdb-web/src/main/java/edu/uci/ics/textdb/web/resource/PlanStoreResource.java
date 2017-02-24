package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.planstore.PlanStore;
import edu.uci.ics.textdb.planstore.PlanStoreConstants;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;
import edu.uci.ics.textdb.web.request.beans.QueryPlanBean;
import edu.uci.ics.textdb.web.response.QueryPlanResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;

/**
 * Created by kishorenarendran on 2/24/17.
 */
@Path("/planstore")
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
public class PlanStoreResource {

    private static PlanStore planStore;
    private static ObjectMapper mapper;

    static {
        try {
            planStore = PlanStore.getInstance();
            mapper = new ObjectMapper();
            mapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        }
        catch (TextDBException e) {
            e.printStackTrace();
        }
    }

    @GET
    public QueryPlanResponse getAllQueryPlans() {
        ArrayList<QueryPlanBean> queryPlans = new ArrayList<>();

        try {
            // Getting an iterator for the plan store
            IDataReader reader = planStore.getPlanIterator();
            reader.open();

            // Iterating through the stored plans, and mapping them to a QueryPlanRequest object
            ITuple tuple;
            while ((tuple = reader.getNextTuple()) != null) {
                String name = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
                String description = tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString();
                String logicalPlanJson = tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString();
                queryPlans.add(new QueryPlanBean(name, description,
                        mapper.readValue(logicalPlanJson, QueryPlanRequest.class)));
            }
        }
        catch (TextDBException | IOException e) {
            e.printStackTrace();
        }

        QueryPlanResponse queryPlanResponse = new QueryPlanResponse(queryPlans);
        return queryPlanResponse;
    }

    @GET
    @Path("/{plan_name}")
    public QueryPlanBean getQueryPlan(@PathParam("plan_name") String planName) {
        try {
            ITuple tuple = planStore.getPlan(planName);
            QueryPlanBean queryPlanBean = new QueryPlanBean(tuple.getField(PlanStoreConstants.NAME).getValue().toString(),
                    tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString(),
                    mapper.readValue(tuple.getField(PlanStoreConstants.LOGICAL_PLAN_JSON).getValue().toString(), QueryPlanRequest.class));
            return queryPlanBean;
        }
        catch (TextDBException | IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    @POST
    public Response addQueryPlan(QueryPlanBean queryPlanBean) {
        try {
            // Adding the query plan to the PlanStore
            planStore.addPlan(queryPlanBean.getName(), queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch (TextDBException | IOException e) {
            e.printStackTrace();
            return Response.status(400).build();
        }

        return Response.status(200).build();
    }

    @DELETE
    @Path("/{plan_name}")
    public Response deleteQueryPlan(@PathParam("plan_name") String planName) {
        try {
            // Deleting the plan from the plan store
            planStore.deletePlan(planName);
        }
        catch(TextDBException e) {
            e.printStackTrace();
            return Response.status(400).build();
        }
        return Response.status(200).build();
    }

    @PUT
    @Path("/{plan_name}")
    public Response updateQueryPlan(@PathParam("plan_name") String planName, QueryPlanBean queryPlanBean) {
        try {
            // Updating the plan in the plan store
            planStore.updatePlan(planName, queryPlanBean.getDescription(),
                    mapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch(JsonProcessingException | TextDBException e) {
            return Response.status(400).build();
        }
        return Response.status(200).build();
    }
}
