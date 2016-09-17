package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;

public class OperatorDAG<T> {
    
    private HashMap<T, HashSet<T>> adjacencyList;
    
    public OperatorDAG() {
        adjacencyList = new HashMap<>();
    }
    
    public void addVertex(T vertex) {
        if (! adjacencyList.containsKey(vertex)) {
            adjacencyList.put(vertex, new HashSet<>());
        }
    }
    
    public void addEdge(T from, T to) {
        if (! adjacencyList.containsKey(from)) {
            adjacencyList.put(from, new HashSet<>());
        }
        adjacencyList.get(from).add(to);
    }
    
    
    public HashMap<T, HashSet<T>> getAdjancyList() {
        return this.adjacencyList;
    }
    
}
