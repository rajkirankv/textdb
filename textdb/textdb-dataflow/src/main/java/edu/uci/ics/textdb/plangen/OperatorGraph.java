package edu.uci.ics.textdb.plangen;

import java.util.HashMap;
import java.util.HashSet;

/**
 * 
 * @author Zuozhi Wang
 *
 * @param <T>
 */
public class OperatorGraph<T> {
    
    private HashMap<T, HashSet<T>> adjacencyList;
    
    public OperatorGraph() {
        adjacencyList = new HashMap<>();
    }
    
    public void addVertex(T vertex) {
        if (! hasVertex(vertex)) {
            adjacencyList.put(vertex, new HashSet<>());
        }
    }
    
    public void addEdge(T from, T to) {
        addVertex(from);
        addVertex(to);
        adjacencyList.get(from).add(to);
    }
    
    public boolean hasVertex(T vertex) {
        return adjacencyList.containsKey(vertex);
    }
    
    
    public HashMap<T, HashSet<T>> getAdjancyList() {
        return this.adjacencyList;
    }
    
}
