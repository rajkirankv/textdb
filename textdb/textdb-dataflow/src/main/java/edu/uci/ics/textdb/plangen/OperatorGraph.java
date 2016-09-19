package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.common.constants.OperatorConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;

/**
 * 
 * @author Zuozhi Wang
 *
 * @param <T>
 */
public class OperatorGraph {
    
    private HashMap<String, IOperator> operatorObjectMap;
    
    private HashMap<String, String> operatorTypeMap;
    private HashMap<String, Map<String, String>> operatorPropertyMap;
    private HashMap<String, HashSet<String>> adjacencyList;

    
    public OperatorGraph() {
        operatorTypeMap = new HashMap<>();
        operatorPropertyMap = new HashMap<>();
        adjacencyList = new HashMap<>();
    }
    
    public void addOperator(String operatorID, String operatorType, Map<String, String> operatorProperties) throws PlanGenException {
        PlanGenUtils.planGenAssert(operatorID != null, "operatorID is null");
        PlanGenUtils.planGenAssert(operatorType != null, "operatorType is null");
        PlanGenUtils.planGenAssert(operatorProperties != null, "operatorProperties is null");
        
        PlanGenUtils.planGenAssert(! operatorID.trim().isEmpty(), "operatorID is empty");
        PlanGenUtils.planGenAssert(! operatorType.trim().isEmpty(), "operatorType is empty");
        
        PlanGenUtils.planGenAssert(! hasOperator(operatorID), "duplicate operatorID: "+operatorID);
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidOperator(operatorType), "invalid operatorType: "+operatorType+", must be one of "+OperatorConstants.operatorList);
  
        operatorTypeMap.put(operatorID, operatorType);
        operatorPropertyMap.put(operatorID, operatorProperties);
        adjacencyList.put(operatorID, new HashSet<>());   
    }
    
    public void addLink(String from, String to) throws PlanGenException {
        PlanGenUtils.planGenAssert(from != null, "\"from\" operator is null");
        PlanGenUtils.planGenAssert(to != null, "\"to\" operator is null");
        
        PlanGenUtils.planGenAssert(! from.trim().isEmpty(), "\"from\" operator is empty");
        PlanGenUtils.planGenAssert(! to.trim().isEmpty(), "\"to\" operator is empty");
        
        PlanGenUtils.planGenAssert(hasOperator(from), "operator " + from + " does not exist");
        PlanGenUtils.planGenAssert(hasOperator(to), "operator " + to + " does not exist");
        
        adjacencyList.get(from).add(to);
    }
       
    public boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }
    
    
    public void buildQueryPlan() throws Exception {
        buildOperators();
        
        validateOperatorGraph();
        
        
    }
    
    private void buildOperators() throws Exception {
        for (String operatorID : adjacencyList.keySet()) {
            String opeartorType = operatorTypeMap.get(operatorID);
            Map<String, String> operatorProperties = operatorPropertyMap.get(operatorID);
            
            if (opeartorType.toLowerCase().endsWith("sink")) {
                System.err.println("TODO: deal with sink");
                continue;
            }        
            IOperator operator = PlanGenUtils.buildOperator(opeartorType, operatorProperties);
            operatorObjectMap.put(operatorID, operator);
        }

    }
    
    /**
     * The operator graph is a DAG (Directed Acyclic Graph).
     * 
     * The operator graph must satisfy the following requirements:
     *   this DAG is weakly connected.
     *   there's no cycles in this DAG.
     *   each operator must meet its input and output arity constraints.
     *   the operator graph has at least one source operator.
     *   the operator graph has exactly one sink.
     *   
     * PlanGenException is thrown if the operator graph fails to meet a requirement.
     * 
     * @throws PlanGenException
     */
    private void validateOperatorGraph() throws PlanGenException {
        checkGraphConnectivity();
        checkGraphCycle();
        checkOperatorInputArity();
        checkOperatorOutputArity();
        
    }
    
    /**
     * This function detects if there are any operators not connected to the operator graph.
     * 
     * It builds an undirected version of the operator graph, and then 
     *   uses a Depth First Search (DFS) algorithm to traverse the graph from any vertex.
     * If the graph is weakly connected, then every vertex should be reached after the traversal.
     * 
     * PlanGenException is thrown if there is an operator not connected to the operator graph.
     * 
     * @throws PlanGenException
     */
    private void checkGraphConnectivity() throws PlanGenException {
        HashMap<String, HashSet<String>> undirectedAdjacencyList = new HashMap<>(adjacencyList);
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                undirectedAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }
        
        String vertex = undirectedAdjacencyList.keySet().iterator().next();
        HashSet<String> unvisitedVertices = new HashSet<>(undirectedAdjacencyList.keySet());
        HashSet<String> visitedVertices = new HashSet<>();
        
        connectivityDfsVisit(vertex, unvisitedVertices, visitedVertices);
        
        if ((! unvisitedVertices.isEmpty()) || 
                visitedVertices.size() != adjacencyList.keySet().size()) {
            throw new PlanGenException("Operators: " + unvisitedVertices + " are not connected to the operator graph.");
        }   
    }
    
    /*
     * This is a helper function for checking connectivity by traversing the graph using DFS algorithm. 
     */
    private void connectivityDfsVisit(String vertex, HashSet<String> unvisitedVertices, HashSet<String> visitedVertices) {
        unvisitedVertices.remove(vertex);       
        for (String adjacentVertex : adjacencyList.get(vertex)) {
            if (unvisitedVertices.contains(adjacentVertex)) {
                connectivityDfsVisit(adjacentVertex, unvisitedVertices, visitedVertices);
            }
        }        
        visitedVertices.add(vertex);
    }
    
    /**
     * This function detects if there are any cycles in the operator graph.
     * 
     * It uses a Depth First Search (DFS) algorithm to traverse the graph.
     * It maintains two lists of visited and visiting vertices.
     * During the traversal, if it reaches an vertex that is in the visiting list, then there's a cycle.
     * 
     * PlanGenException is thrown if a cycle is detected in the graph.
     * 
     * @throws PlanGenException
     */
    private void checkGraphCycle() throws PlanGenException {
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> visitingVertices = new HashSet<>();
        
        for (String vertex : adjacencyList.keySet()) {
            if (unvisitedVertices.contains(vertex)) {
                checkCycleDfsVisit(vertex, unvisitedVertices, visitingVertices);
            }
        }
    }
    
    /*
     * This is a helper function for detecting cycles by traversing the graph the graph using DFS algorithm. 
     */
    private void checkCycleDfsVisit(String vertex, HashSet<String> unvisitedVertices, 
            HashSet<String> visitingVertices) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        visitingVertices.add(vertex);
        
        for (String adjacentVertex : adjacencyList.get(vertex)) {
            if (visitingVertices.contains(adjacentVertex)) {
                throw new PlanGenException("The following operators form a cycle in operator graph: "+visitingVertices);
            }
            if (unvisitedVertices.contains(adjacentVertex)) {
                checkCycleDfsVisit(adjacentVertex, unvisitedVertices, visitingVertices);
            }
        }
        
        visitingVertices.remove(vertex);
    }
    
    
    private void checkOperatorOutputArity() {
        for (String key : adjacencyList.keySet()) {
            
        }
        
        
    }
    
    private void checkOperatorInputArity() {
        HashMap<String, HashSet<String>> transposeAdjacencyList = new HashMap<>();
        for (String key : adjacencyList.keySet()) {
            transposeAdjacencyList.put(key, new HashSet<>());
        }
        for (String key : adjacencyList.keySet()) {
            for (String value : adjacencyList.get(key)) {
                transposeAdjacencyList.get(value).add(key);
            }
        }
    }
    
    
    
    private void checkSource() {
        
    }
    
    private void checkSink() {
        
    }
    
    
    


    
}
