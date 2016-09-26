package wsu.eecs.mlkd.KGQuery.algo;

//this class calculate the number of different types of relationships

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



public class Neo4jTypeCounting {
	
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
		int i=0,wholeTypesNumber=0;
		for(i=0;i<100000;i++)
			relationshipNumber[i] = 0;
		
		PrintWriter pw = new PrintWriter(new FileWriter("/Users/qsong/Downloads/aaa.txt"));  
		
		Neo4jTypeCounting neo4jTypeCounting = new Neo4jTypeCounting();
		neo4jTypeCounting.setUp();
				
		try ( Transaction tx = neo4jTypeCounting.graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(neo4jTypeCounting.graphDb);
			for (Relationship n : globalOperation.getAllRelationships())
			{
				RelationshipType relationshipType = n.getType();
				if (relationshipName[0] == null){
					relationshipName[0] = relationshipType.name();
					relationshipNumber[0] ++;
					wholeTypesNumber ++;
				}
				else{
					boolean flag = false;
					for(i=0;i<wholeTypesNumber;i++)
					{
						if(relationshipName[i] == relationshipType.name())
						{
							relationshipNumber[i] ++;
							flag = true;
							break;
						}
					}
					if(flag == false){
						relationshipName[wholeTypesNumber] = relationshipType.name();
						relationshipNumber[wholeTypesNumber++] ++;
					}
				}
			}
			sort(relationshipNumber,relationshipName,wholeTypesNumber);
			for (i=0;i<wholeTypesNumber;i++){
				pw.println(relationshipName[i] + "\t" +relationshipNumber[i]);
				System.out.println(relationshipName[i] + "\t" +relationshipNumber[i]);
				aaa += relationshipNumber[i];
			}
			pw.close();
			System.out.println("wholeTypesNumber = " + wholeTypesNumber);
			System.out.println("aaa ="  + aaa);
			tx.success();
		}		
		neo4jTypeCounting.shutdown();
	}
	
	public static void sort(int[] numbers, String[] types, int length){
        for (int i = 1; i < length; i++) {
            int currentNumber = numbers[i];
            String type = types[i];
            int j = i - 1;
            while (j >= 0 && numbers[j] < currentNumber) {
                numbers[j + 1] = numbers[j];
                types[j + 1] = types[j];
                j--;
            }
            numbers[j + 1] = currentNumber;
            types[j + 1] = type;
        }
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
