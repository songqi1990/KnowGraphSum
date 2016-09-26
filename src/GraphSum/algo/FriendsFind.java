package wsu.eecs.mlkd.KGQuery.algo;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;

public class FriendsFind
{
    public String printNeoFriends(GraphDatabaseService graphDb, Node node, RelationshipType relationshiptype)
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // START SNIPPET: friends-usage
            int numberOfFriends = 0;
            String output = node.getProperty( "name" ) + "'s friends:\n";
            Traverser friendsTraverser = getFriends( graphDb, node, relationshiptype );
            for ( Path friendPath : friendsTraverser )
            {
                output += "At depth " + friendPath.length() + " => "
                          + friendPath.endNode()
                                  .getProperty( "name" ) + "\n";
                numberOfFriends++;
            }
            output += "Number of friends found: " + numberOfFriends + "\n";
            // END SNIPPET: friends-usage
            return output;
        }
    }

    // START SNIPPET: get-friends
    private Traverser getFriends(GraphDatabaseService graphDb, final Node person, RelationshipType relationshiptype )
    {
        TraversalDescription td = graphDb.traversalDescription()
                .breadthFirst()
                .relationships( relationshiptype, Direction.OUTGOING )
                .evaluator( Evaluators.excludeStartPosition() );
        return td.traverse( person );
    }
   
}