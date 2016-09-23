package edu.uci.ics.textdb.plangen;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import edu.uci.ics.textdb.api.dataflow.IOperator;
import edu.uci.ics.textdb.api.dataflow.ISink;
import edu.uci.ics.textdb.api.plan.Plan;
import edu.uci.ics.textdb.common.constants.OperatorConstants;
import edu.uci.ics.textdb.common.exception.PlanGenException;
import edu.uci.ics.textdb.dataflow.connector.OneToNBroadcastConnector;

/**
 * 
 * @author Zuozhi Wang
 *
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
        PlanGenUtils.planGenAssert(PlanGenUtils.isValidOperator(operatorType), 
                String.format("%s is an invalid operator type, it must be one of %s.", 
                        operatorType, OperatorConstants.operatorList.toString()));
  
        operatorTypeMap.put(operatorID, operatorType);
        operatorPropertyMap.put(operatorID, operatorProperties);
        adjacencyList.put(operatorID, new HashSet<>());   
    }
    
    public void addLink(String from, String to) throws PlanGenException {
        PlanGenUtils.planGenAssert(from != null, "\"from\" operator is null");
        PlanGenUtils.planGenAssert(to != null, "\"to\" operator is null");
        
        PlanGenUtils.planGenAssert(! from.trim().isEmpty(), "\"from\" operator is empty");
        PlanGenUtils.planGenAssert(! to.trim().isEmpty(), "\"to\" operator is empty");
        
        PlanGenUtils.planGenAssert(hasOperator(from), String.format("operator %s doesn't exist", from));
        PlanGenUtils.planGenAssert(hasOperator(to), String.format("operator %s doesn't exist", to));
        
        adjacencyList.get(from).add(to);
    }
       
    public boolean hasOperator(String operatorID) {
        return adjacencyList.containsKey(operatorID);
    }
    
    
    public Plan buildQueryPlan() throws Exception {
        buildOperators();        
        validateOperatorGraph();
        buildLinks();
        ISink sink = getSinkOperator();
        
        Plan queryPlan = new Plan(sink);
        return queryPlan;
    }
    


    private void buildOperators() throws Exception {
        for (String operatorID : adjacencyList.keySet()) {
            String opeartorType = operatorTypeMap.get(operatorID);
            Map<String, String> operatorProperties = operatorPropertyMap.get(operatorID);
            
            IOperator operator = PlanGenUtils.buildOperator(opeartorType, operatorProperties);
            operatorObjectMap.put(operatorID, operator);
        }

    }
    
    /*
     * This function validates the operator graph.
     * It throws a PlanGenException if the operator graph fails to meet any of the requirements.
     * 
     * The operator graph is a DAG (Directed Acyclic Graph).
     * 
     * The operator graph must satisfy the following requirements:
     *   this DAG is weakly connected.
     *   there's no cycles in this DAG.
     *   each operator must meet its input and output arity constraints.
     *   the operator graph has at least one source operator.
     *   the operator graph has exactly one sink.
     * 
     */
    private void validateOperatorGraph() throws PlanGenException {
        checkGraphConnectivity();
        checkGraphCycle();
        checkOperatorInputArity();
        checkOperatorOutputArity();
        checkSourceOperator();
        checkSinkOperator();
    }
    
    
    private void buildLinks() throws PlanGenException {
        // pick a source operator as a starting point of graph traversal
        String startVertex = adjacencyList.keySet().stream()
                .filter(vertex -> operatorTypeMap.get(vertex).toLowerCase().contains("source"))
                .findAny().orElse(null);
        PlanGenUtils.planGenAssert(startVertex != null, "Build links error: operator graph doesn't have a source operator.");
        
        HashSet<String> unvisitedVertices = new HashSet<>(adjacencyList.keySet());
        HashSet<String> visitedVertices = new HashSet<>();
                
        while (true) {
            buildLinkDfsVisit(startVertex, unvisitedVertices, visitedVertices);
            
            if (unvisitedVertices.isEmpty()) {
                break;
            } else {
                startVertex = unvisitedVertices.iterator().next();
            }
        }
    }
    
    private void buildLinkDfsVisit(String vertex, HashSet<String> unvisitedVertices, HashSet<String> visitedVertices) throws PlanGenException {
        unvisitedVertices.remove(vertex);
        IOperator currentOperator = operatorObjectMap.get(vertex);
        int outputArity = adjacencyList.get(vertex).size();
        if (outputArity > 1) {
            OneToNBroadcastConnector oneToNConnector = new OneToNBroadcastConnector(outputArity);
            oneToNConnector.setInputOperator(currentOperator);
            int counter = 0;
            for (String adjacentVertex : adjacencyList.get(vertex)) {
                IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                handleSetInputOperator(oneToNConnector.getOutputOperator(counter), adjacentOperator);
                
                if (unvisitedVertices.contains(adjacentVertex)) {
                    connectivityDfsVisit(adjacentVertex, unvisitedVertices, visitedVertices);
                }
            }
        } else {
            for (String adjacentVertex : adjacencyList.get(vertex)) {
                IOperator adjacentOperator = operatorObjectMap.get(adjacentVertex);
                handleSetInputOperator(currentOperator, adjacentOperator);

                if (unvisitedVertices.contains(adjacentVertex)) {
                    connectivityDfsVisit(adjacentVertex, unvisitedVertices, visitedVertices);
                }
            }
        }
        
        visitedVertices.add(vertex);      
    }
      
    private ISink getSinkOperator() throws PlanGenException {
        IOperator sinkOperator = adjacencyList.keySet().stream()
                .filter(operator -> operatorTypeMap.get(operator).toLowerCase().contains("sink"))
                .map(operator -> operatorObjectMap.get(operator))
                .findFirst().orElse(null);
        
        PlanGenUtils.planGenAssert(sinkOperator != null, "Error: sink operator doesn't exist.");
        PlanGenUtils.planGenAssert(sinkOperator instanceof ISink, "Error: sink operator's type doesn't match.");
        
        return (ISink) sinkOperator;
    }
    
    
    private void handleSetInputOperator(Object from, Object to) throws PlanGenException {
        try {
            to.getClass().getMethod("setInputOperator", IOperator.class).invoke(to, from);
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException 
                | IllegalArgumentException | InvocationTargetException e) {
            throw new PlanGenException(e.getMessage(), e);
        }  
    }
    
        
    /*
     * This function detects if there are any operators not connected to the operator graph.
     * 
     * It builds an undirected version of the operator graph, and then 
     *   uses a Depth First Search (DFS) algorithm to traverse the graph from any vertex.
     * If the graph is weakly connected, then every vertex should be reached after the traversal.
     * 
     * PlanGenException is thrown if there is an operator not connected to the operator graph.
     * 
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
    
    /*
     * This function detects if there are any cycles in the operator graph.
     * 
     * It uses a Depth First Search (DFS) algorithm to traverse the graph.
     * It maintains two lists of visited and visiting vertices.
     * During the traversal, if it reaches an vertex that is in the visiting list, then there's a cycle.
     * 
     * PlanGenException is thrown if a cycle is detected in the graph.
     * 
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
    
    /*
     * This function checks if the input arities of all operators match the expected input arities.
     */
    private void checkOperatorInputArity() throws PlanGenException {
        HashMap<String, HashSet<String>> transposeAdjacencyList = new HashMap<>();
        for (String vertex : adjacencyList.keySet()) {
            transposeAdjacencyList.put(vertex, new HashSet<>());
        }
        for (String vertexOrigin : adjacencyList.keySet()) {
            for (String vertexDestination : adjacencyList.get(vertexOrigin)) {
                transposeAdjacencyList.get(vertexDestination).add(vertexOrigin);
            }
        }
        
        for (String vertex : transposeAdjacencyList.keySet()) {
            int actualInputArity = transposeAdjacencyList.get(vertex).size();
            int expectedInputArity = OperatorArityConstants.fixedInputArityMap.get(operatorTypeMap.get(vertex));
            PlanGenUtils.planGenAssert(
                    actualInputArity == expectedInputArity,
                    String.format("Operator %s should have %d inputs, got %d.", vertex, expectedInputArity, actualInputArity));
        }
    }
    
    /*
     * This function checks if the output arity of "sink" operator match.
     * 
     * For other operators, output arity is not checked,  
     * because an one to N connector will be automatically added if necessary.
     * 
     */
    private void checkOperatorOutputArity() throws PlanGenException {
        for (String vertex : adjacencyList.keySet()) {
            int actualOutputArity = adjacencyList.get(vertex).size();
            int expectedOutputArity = OperatorArityConstants.fixedOutputArityMap.get(operatorTypeMap.get(vertex).toLowerCase());

            if (vertex.toLowerCase().contains("sink")) {
                PlanGenUtils.planGenAssert(
                        actualOutputArity == expectedOutputArity,
                        String.format("Sink %s should have %d output links, got %d.", vertex, expectedOutputArity, actualOutputArity));
            } else {
                PlanGenUtils.planGenAssert(
                        actualOutputArity != 0,
                        String.format("Operator %s should have at least %d output links, got 0.", vertex, expectedOutputArity)); 
            }
        }
    }
    
    /*
     * This function makes sure that the operator graph has at least one source operator
     */
    private void checkSourceOperator() throws PlanGenException {
        boolean sourceExist = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .anyMatch(type -> type.toLowerCase().contains("source"));
        
        PlanGenUtils.planGenAssert(sourceExist, "There must be at least one source operator.");
    }
    
    /*
     * This function makes sure that the operator graph has exactly one sink operator.
     */
    private void checkSinkOperator() throws PlanGenException {
        long sinkOperatorNumber = adjacencyList.keySet().stream()
                .map(operator -> operatorTypeMap.get(operator))
                .filter(operatorType -> operatorType.toLowerCase().contains("sink"))
                .count();
        
        PlanGenUtils.planGenAssert(sinkOperatorNumber == 1, 
                String.format("There must be exaxtly one sink operator, got %d.", sinkOperatorNumber));
    }
 
}
