package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;

public class NodeDistance
{
    public int calculateNodeDistance(GraphDatabaseService graphDb, Node node1, Node node2, String nodePropertyKey )
    {
        
            // START SNIPPET: shortestPathUsage
            PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                    PathExpanders.forDirection(Direction.BOTH ), 4000 );
            Path foundPath = finder.findSinglePath( node1, node2 );
            if (foundPath == null)
            	return 0;
            else
            	return foundPath.length();
            // END SNIPPET: shortestPathUsage
        }
    
}
