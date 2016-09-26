package wsu.eecs.mlkd.KGQuery.algo;

import java.util.HashSet;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.tooling.GlobalGraphOperations;

public class TriangleCounting {
	public void calculateTriangleCounting(GraphDatabaseService graphDb) {

		long tc = 0;
		
		
		Iterable<Node> nodes = GlobalGraphOperations.at(graphDb).getAllNodes();
		HashSet<Node> set = new HashSet<Node>();

		for (Node u : nodes) {
			
			set.clear();
			for (Relationship r : u.getRelationships(Direction.BOTH)) {
				Node v = r.getOtherNode(u);
				set.add(v);
			}
			
			for (Relationship r : u.getRelationships(Direction.BOTH)) {
				Node v = r.getOtherNode(u);
				
				for (Relationship r2 : v.getRelationships(Direction.BOTH)) {
					Node w = r2.getOtherNode(v);
					if (u.getId() < v.getId() && v.getId() < w.getId()) {
						if (set.contains(w)) tc++;
					}
				}
			}
		}

		
		System.out.println("Result : " + tc); 
	}
}
