package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.graphdb.*;

public class Neo4jSubGraph {
	
	Set<Node> nodes;
	Set<Relationship> edges;
	String supergraph;
	
	public Neo4jSubGraph(Iterable<Node> nodes, Iterable<Relationship> edges) {
		this.nodes = Neo4jGraph.IterToSet(nodes);
		this.edges = Neo4jGraph.IterToSet(edges);
	}
	
	public Neo4jSubGraph(Set<Node> nodes, Set<Relationship> edges) {
		this.nodes = nodes;
		this.edges = edges;
	}
	
	public Neo4jSubGraph() { 
		this.nodes = new HashSet<Node>();
		this.edges = new HashSet<Relationship>();
	}
	
	public void addVertex(Node n) {
		nodes.add(n);
	}
	
	public boolean addEdge(Relationship r) {
		if (nodes.contains(r.getStartNode()) && nodes.contains(r.getEndNode()) ) {
			edges.add(r);	
			return true;
		}
		return false;
	}
	
	public boolean hasNode(Node a) {
		return nodes.contains(a);
	}
	
	public boolean hasEdge(Relationship r) {
		return edges.contains(r);
	}
	
	public Relationship getEdge(Node a, Node b) {
		for (Relationship r : edges ) {
			if (r.getStartNode().equals(a) && r.getEndNode().equals(b)) {
				return r;
			}
		}
		return null;
	}
	
	public void addEdgeWithNodes(Relationship r) {
		if (!nodes.contains(r.getStartNode()))  {
			this.addVertex(r.getStartNode());
		} 
		if (!nodes.contains(r.getEndNode()) ) {
			this.addVertex(r.getEndNode());
		}
		edges.add(r);	
	}
	
	/**
	 * this removes vertex and its relationships
	 */
	public boolean removeVertex(Node n) {
		boolean b = nodes.remove(n);
		for (Relationship r : edges) {
			if (r.getEndNode().equals(n) || r.getStartNode().equals(n)) {
				edges.remove(r);
			}
		}
		return b;
	}
	
	public boolean removeEdge(Relationship r) {
		return edges.remove(r);
	}
	
	public void setSuperGraph(String file) {
		this.supergraph = file;
	}
	
	public int getNodeNumber() {
		return nodes.size();
	}
	
	public int getEdgeNumber() {
		return edges.size();
	}
	
	public void ClearNodeWeight(){
		for (Node n : nodes) {
			if (n.hasProperty("weight")) {
				n.removeProperty("weight");
			}
		}
	}
	
	public Set<Node> getNodes() {
		return nodes;
	}
	
	public Set<Relationship> getEdges() {
		return edges;
	}

	public Set<Relationship> getAllEdges(Node a, Node b) {
		Set<Relationship> rels = new HashSet<Relationship>();
		for (Relationship r : edges) {
			Node x = r.getStartNode();
			Node y = r.getEndNode();
			
			if (a.equals(x) || a.equals(y) || b.equals(x) || b.equals(y) ) {
				rels.add(r);
			}
		}
		return rels;
	}

}
