package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
public class Reachability {
	public  boolean calcualteReachability(GraphDatabaseService graphDb, Node node1, Node node2, String nodePropertyKey )
    {
		PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                PathExpanders.forDirection(Direction.BOTH ), 40000 );
        Path foundPath = finder.findSinglePath( node1, node2 );
        	if (foundPath == null)
        		return false;
        	else 
        		return true;
    }
}
