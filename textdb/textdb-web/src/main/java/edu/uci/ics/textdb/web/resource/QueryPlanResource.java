package edu.uci.ics.textdb.web.resource;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.uci.ics.textdb.api.common.ITuple;
import edu.uci.ics.textdb.api.exception.TextDBException;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.api.storage.IDataReader;
import edu.uci.ics.textdb.common.utils.Utils;
import edu.uci.ics.textdb.dataflow.sink.TupleStreamSink;
import edu.uci.ics.textdb.engine.Engine;
import edu.uci.ics.textdb.planstore.PlanStore;
import edu.uci.ics.textdb.planstore.PlanStoreConstants;
import edu.uci.ics.textdb.web.request.QueryPlanRequest;
import edu.uci.ics.textdb.web.request.beans.QueryPlanBean;
import edu.uci.ics.textdb.web.response.QueryPlanResponse;
import edu.uci.ics.textdb.web.response.SampleResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class will be the resource class for accepting a query plan edu.uci.ics.textdb.web.request and executing the
 * query plan to get the query response
 * Created by kishorenarendran on 10/17/16.
 */
@Path("/queryplan")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class QueryPlanResource {

    /**
     * This is the edu.uci.ics.textdb.web.request handler for the execution of a Query Plan.
     * @param queryPlanRequest - An object that models the query plan edu.uci.ics.textdb.web.request that will be POSTed
     * @return - Generic response object for now, which wjust verifies the creation of operator properties' hashmap
     * @throws Exception
     */
    @POST
    @Path("/execute")
    public Response executeQueryPlan(QueryPlanRequest queryPlanRequest) throws Exception {
        // Aggregating all the operator properties, and creating a logical plan object
        boolean aggregatePropertiesFlag = queryPlanRequest.aggregateOperatorProperties();
        boolean createLogicalPlanFlag = queryPlanRequest.createLogicalPlan();

        ObjectMapper objectMapper = new ObjectMapper();
        if(aggregatePropertiesFlag && createLogicalPlanFlag) {
            // generate the physical plan
            Plan plan = queryPlanRequest.getLogicalPlan().buildQueryPlan();
            
            // if the sink is TupleStreamSink, send the response back to front-end
            if (plan.getRoot() instanceof TupleStreamSink) {
                TupleStreamSink sink = (TupleStreamSink) plan.getRoot();
                
                // get all the results and send them to front-end
                // returning all the results at once is a **temporary** solution
                // TODO: in the future, request some number of results at a time
                sink.open();
                List<ITuple> results = sink.collectAllTuples();
                sink.close();
                
                SampleResponse sampleResponse = new SampleResponse(0, Utils.getTupleListJSON(results).toString());
                return Response.status(200)
                        .entity(objectMapper.writeValueAsString(sampleResponse))
                        .build();
                
            } else {
                // if the sink is not TupleStreamSink, execute the plan directly
                Engine.getEngine().evaluate(plan);    
                SampleResponse sampleResponse = new SampleResponse(0, "Plan Successfully Executed");
                return Response.status(200)
                        .entity(objectMapper.writeValueAsString(sampleResponse))
                        .build();
            }
        }
        else {
            // Temporary sample response when the operator properties aggregation does not function
            SampleResponse sampleResponse = new SampleResponse(1, "Unsuccessful");
            return Response.status(400)
                    .entity(objectMapper.writeValueAsString(sampleResponse))
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "OPTIONS,GET,PUT,POST,DELETE,HEAD")
                    .header("Access-Control-Allow-Headers", "X-Requested-With,Content-Type,Accept,Origin")
                    .header("Access-Control-Max-Age", "1728000")
                    .build();
        }
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public QueryPlanResponse getQueryPlans() {
        ArrayList<QueryPlanBean> queryPlans = new ArrayList<>();

        try {
            // Getting an iterator for the plan store
            PlanStore planStore = PlanStore.getInstance();
            planStore.createPlanStore();
            IDataReader reader = planStore.getPlanIterator();
            reader.open();

            // ObjectMapper created for translating logical plan JSON to an object
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
            mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            // Iterating through the stored plans, and mapping them to a QueryPlanRequest object
            ITuple tuple;
            while ((tuple = reader.getNextTuple()) != null) {
                String name = tuple.getField(PlanStoreConstants.NAME).getValue().toString();
                String description = tuple.getField(PlanStoreConstants.DESCRIPTION).getValue().toString();
                String filePath = tuple.getField(PlanStoreConstants.FILE_PATH).getValue().toString();
                String logicalPlanJson = planStore.readPlanJson(filePath);
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

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public Response addQueryPlan(String body) {
        try {
            // Getting an instance of PlanStore and creating a PlanStore if it doesn't already exist
            PlanStore planStore = PlanStore.getInstance();
            planStore.createPlanStore();

            // ObjectMapper instance created to get Query Plan JSON string
            ObjectMapper objectMapper = new ObjectMapper();
            objectMapper.configure(MapperFeature.USE_GETTERS_AS_SETTERS, false);
            objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

            // Adding the query plan to the PlanStore
            QueryPlanBean queryPlanBean = objectMapper.readValue(body, QueryPlanBean.class);
            planStore.addPlan(queryPlanBean.getName(), queryPlanBean.getDescription(),
                     objectMapper.writeValueAsString(queryPlanBean.getQueryPlan()));
        }
        catch (TextDBException | IOException e) {
            e.printStackTrace();
            return Response.status(400).build();
        }

        return Response.status(200).build();
    }
}