package wsu.eecs.mlkd.KGQuery.algo;

//this class calculate the average number of different types of relationships for each nodes

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.traversal.Evaluators;
import org.neo4j.graphdb.traversal.TraversalDescription;
import org.neo4j.graphdb.traversal.Traverser;
import org.neo4j.graphdb.index.Index;



public class Neo4jTypesRelNodes {
	
	//private static final String DB_PATH = "/Users/qsong/Downloads/dbpedia_infobox_properties_en.db";
	//private static final String DB_PATH = "/Users/qsong/Downloads/graph.db";
	//private static final String DB_PATH = "/Users/qsong/Downloads/YagoCores_graph.db";
	private static final String DB_PATH = "/Users/qsong/Downloads/social_network_factor_1.db";
	private GraphDatabaseService graphDb;
	private static final String NAME_KEY = "name";

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
		long aaa = 0;
		String relationshipName[] = new String[100000];
		int relationshipNumber[] = new int[100000];
		int i=0,wholeTypesNumber=0,res=0;
		for(i=0;i<100000;i++)
			relationshipNumber[i] = 0;
		
		PrintWriter pw = new PrintWriter(new FileWriter("/Users/qsong/Downloads/aaa.txt"));  
		
		Neo4jTypesRelNodes neo4jTypesRelNodes = new Neo4jTypesRelNodes();
		neo4jTypesRelNodes.setUp();
		
		try ( Transaction tx = neo4jTypesRelNodes.graphDb.beginTx() )
		{
			String relName[] = new String[100000];
			int num = 0;
			int relNum = 0;
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(neo4jTypesRelNodes.graphDb);
			for (Node n : globalOperation.getAllNodes()){
				num = 0;
				relNum = 0;
				relName[0] = null;
				Iterable<Relationship> relNodes = n.getRelationships();
				for (Relationship r : relNodes){
					if(relName[0] == null){
						relName[num++] = r.getType().name();
					}
					else{
						boolean flag = false;
						for(int j = 0; j < num; j ++)
						{
							if(relName[j] == r.getType().name()){
								flag = true;
								break;
							}
						}
						if (flag == false)
							relName[num++] = r.getType().name();
					}
					relNum ++;
				}
				pw.println(num + "\t" + relNum);
				//System.out.println("num = \t" + num + "\t relNum =\t" + relNum);
				res += num;
			}
			System.out.println("res = " + res);
			pw.close();
			tx.success();
		}
		neo4jTypesRelNodes.shutdown();
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
