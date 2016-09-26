package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.tooling.GlobalGraphOperations;

public class GraphDensity {
	public double calculateGraphDensity(GraphDatabaseService graphDb)
	{
		int numberOfVertices,numberOfRelationships;
		double density;
		numberOfVertices = IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllNodes());
		numberOfRelationships = IteratorUtil.count(GlobalGraphOperations.at(graphDb).getAllRelationships());
		System.out.println("number of vertices " + numberOfVertices);
		System.out.println("number of relationships " + numberOfRelationships);
		density = ((double)numberOfRelationships/(double)numberOfVertices)/((double)numberOfVertices-1);
		System.out.printf("%1.18f", density);
		return density;
	}
}
