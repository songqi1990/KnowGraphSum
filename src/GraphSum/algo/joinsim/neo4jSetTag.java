package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.IOException;

import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;

import org.neo4j.graphdb.Transaction;

public class neo4jSetTag {
	
	private static final String DB_PATH = "/Users/qsong/Downloads/query.db";
	private GraphDatabaseService graphDb;

	
	public void setUp() throws IOException
	{
	//FileUtils.deleteRecursively( new File( DB_PATH ) );
	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
	registerShutdownHook(graphDb);
	}
	
	public void shutdown()
	{
	graphDb.shutdown();
	}
	
	
	public static void main(String[] args) throws IOException {
		neo4jSetTag graphneo4j = new neo4jSetTag();
		graphneo4j.setUp();
		
		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
			for (Node n : globalOperation.getAllNodes())
			{
				//if(n.getProperty("tag") == null)
					n.setProperty("tag", n.getId());
			}
			tx.success();
		}
		graphneo4j.shutdown();
	}
	
	private static void registerShutdownHook( final GraphDatabaseService graphDb )
	{
	    // Registers a shutdown hook for the Neo4j instance so that it
	    // shuts down nicely when the VM exits (even if you "Ctrl-C" the
	    // running application).
	    Runtime.getRuntime().addShutdownHook( new Thread()
	    {
	        @Override
	        public void run()
	        {
	            graphDb.shutdown();
	        }
	    } );
	}
	
	
}
