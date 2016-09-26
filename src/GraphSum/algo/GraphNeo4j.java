package wsu.eecs.mlkd.KGQuery.algo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

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
import org.neo4j.index.lucene.unsafe.batchinsert.LuceneBatchInserterIndexProvider;


public class GraphNeo4j {
	
	//private static final String DB_PATH = "/Users/qsong/Downloads/dbpedia_infobox_properties_en.db";
	//private static final String DB_PATH = "/Users/qsong/Downloads/social_network_factor_1.db";
//	nprivate static final String DB_PATH = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db";
//	private static final String DB_PATH = "/Users/qsong/Downloads/Simulation_test/test_query_1.db";
	private static final String DB_PATH = "/home/qsong/data/neo4j/dbpedia.db";
//	private static final String DB_PATH = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup";
	private GraphDatabaseService graphDb;
	private static final String NAME_KEY = "name";

	
	public void setUp(String PATH) throws IOException
	{
	//FileUtils.deleteRecursively( new File( DB_PATH ) );
	graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( PATH );
	registerShutdownHook(graphDb);
	}
	
	public void shutdown()
	{
	graphDb.shutdown();
	}
	
	
	public static void main(String[] args) throws IOException {
		GraphNeo4j graphneo4j = new GraphNeo4j();
		graphneo4j.setUp(args[0]);
//		graphneo4j.setUp(DB_PATH);
		int nodeNumber = 0;
		int relationshipNumber = 0;
		int deleteNumber = 0;
		long nodeId = 0;
		int zeroDegreeNodeNumber = 0;
		int nullLabelsNumber = 0;
		long wholeProperties = 0;
		long wholeLabels = 0;
		Relationship relationshipdelete = null;
		int multipleEdges = 0;
		int number = 0;
		Hashtable<Long,Hashtable<String,Integer>> hop1Index = new Hashtable<Long,Hashtable<String,Integer>>();
		Hashtable<Long,Hashtable<String,Integer>> hop2Index = new Hashtable<Long,Hashtable<String,Integer>>();
		HashSet<Node> hop1Node = new HashSet<Node>();
		
		
//		try(Transaction tx = graphneo4j.graphDb.beginTx()){
//			
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			Iterable<Relationship> rela  = globalOperation.getAllRelationships();
//			for(Relationship r:rela){
//				for(Label l1 : r.getStartNode().getLabels()){
//					for(Label l2: r.getEndNode().getLabels()){
//						if(l1.name().equals(args[0]) && l2.name().equals(args[1])){
//							for(String s : r.getAllProperties().keySet())
//								System.out.println("edge: " + r.getAllProperties().get(s));
//							for(String s1 : r.getStartNode().getAllProperties().keySet())
//								System.out.println("from node: " + s1 + ": " +r.getStartNode().getAllProperties().get(s1));
//							for(String s2 : r.getEndNode().getAllProperties().keySet())
//								System.out.println("to node: " + s2 + ": " +r.getEndNode().getAllProperties().get(s2));
//						}
//					}
//				}
//			}
//			tx.success();
//		}
		//---------------------------------------------------------
		//this part deletes independent nodes
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
//			for ( Node item : neoNodes )
//			 {
//				int a = 0;
//				nodeNumber ++;
////				if (nodeNumber >= 5000000)
////					break;
//				nodeId=item.getId();
//				for(Relationship r : item.getRelationships())
//					a++;
//				if (a == 0){
//					//System.out.println("node Id: " + nodeId);
//					Node nodedelete = graphneo4j.graphDb.getNodeById(nodeId);
//					//System.out.println("Node?: " + nodedelete.toString() + " " + nodedelete.getId()); 
//					nodedelete.delete();
//					deleteNumber++;
//					if(deleteNumber%10000 == 0)
//						System.out.println(deleteNumber);
//					if (deleteNumber > 100000)
//						break;
//				}
//			 }
//			for ( Node item : neoNodes)
//			{
//				int a = 0;
//				for(Relationship r : item.getRelationships())
//					a++;
//				if (a==0)
//					zeroDegreeNodeNumber ++;
//			}
//			System.out.println(deleteNumber);
//			System.out.println("deal with" + nodeNumber);
//			System.out.println("all independent nodes: " + zeroDegreeNodeNumber);
//			tx.success();
//		}
		
		//---------------------------------------------------------
		//delete nodes with small degree and related relationships
		//---------------------------------------------------------
		/*
		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
			for (Node item : neoNodes )
			 {
				
				if (number >= 80000)
					break;
				nodeId=item.getId();
				if ((item.getDegree() < 10) && (nodeId > 4393388)){
					number ++;
					Node nodedelete = graphneo4j.graphDb.getNodeById(nodeId); 
					nodedelete.delete();
					Iterable<Relationship> itemRelationships = item.getRelationships();
					for (Relationship ritem : itemRelationships)
					{
						long relationshipId = ritem.getId();
						try 
						{
							relationshipdelete = graphneo4j.graphDb.getRelationshipById(relationshipId);
							relationshipdelete.delete();
						}
						catch (NotFoundException e){
							
						}
						
					}
					deleteNumber++;
					if (deleteNumber%10000 == 0)
						System.out.println("delete "+deleteNumber+ " node");
				}
			 }
			System.out.println("current node id: " + nodeId);
			for ( Node item : neoNodes)
			{
				if (item.getDegree() < 10)
					zeroDegreeNodeNumber ++;
			}
			System.out.println(deleteNumber);
			System.out.println("deal with" + number);
			System.out.println("still need to delete : " + zeroDegreeNodeNumber);
			tx.success();
		}
		*/
	
		//---------------------------------------------------------
		//delete nodes with no labels
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			HashSet<Relationship> tobedelete = new HashSet<Relationship>();
//			HashSet<Node> ntobedelete = new HashSet<Node>();
//			int nodeLabelNumber;
//			int tobeDelete=0;
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
//			for (Node item : neoNodes )
//			 {
//				nodeLabelNumber = 0;
//				if (number >= 1000000)
//					break;
//				nodeId=item.getId();
////				System.out.println("current node id: " + nodeId);
//				Iterable<Label> labels = item.getLabels();
//				for(Label label : labels)
//					nodeLabelNumber ++;
//				if(nodeLabelNumber == 0){
//					number ++;
//					ntobedelete.add(item);
//					Iterable<Relationship> itemRelationships = item.getRelationships();
//					for (Relationship ritem : itemRelationships)
//					{
//						long relationshipId = ritem.getId();
//						try 
//						{
//							relationshipdelete = graphneo4j.graphDb.getRelationshipById(relationshipId);
//							tobedelete.add(relationshipdelete);
//						}
//						catch (NotFoundException e){
//							
//						}
//						
//					}
//					deleteNumber++;
//					if (deleteNumber%10000 == 0)
//						System.out.println("delete "+deleteNumber+ " node");
//				}
//			 }
//			for(Node n : ntobedelete)
//				n.delete();
//			for(Relationship r : tobedelete)
//				r.delete();
//			for (Node item : neoNodes )
//			 {
//				nodeLabelNumber = 0;
//				nodeId=item.getId();
//				Iterable<Label> labels = item.getLabels();
//				for(Label label : labels)
//					nodeLabelNumber ++;
//				if(nodeLabelNumber == 0)
//					tobeDelete++;
//			 }
//			System.out.println(deleteNumber);
//			System.out.println("deal with" + number);
//			System.out.println("Still need to delete: " + tobeDelete);
//			tx.success();
//		}
		
		
		//---------------------------------------------------------
		//This part calculates the number of nodes with different degrees
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
//			//ResourceIterable<String> allPropertyKeys = globalOperation.getAllPropertyKeys();
//			for ( Node item : neoNodes )
//			 {
//				int a = 0;
//				for(Relationship r : item.getRelationships())
//					a++;
//				if(a==1)
//					zeroDegreeNodeNumber ++;
//			 }
//			tx.success();
//			System.out.println(zeroDegreeNodeNumber);
//		}
		
		
		//---------------------------------------------------------
		//This part calculates the Average properties per Node
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			nodeNumber = 0;
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			int properties;
//			for (Node n : globalOperation.getAllNodes())
//			{
//				properties = 0;
//				nodeNumber ++;
//				Iterable<String> keys = n.getPropertyKeys();
//				for(String key : keys)
//					properties ++;
//				wholeProperties += properties;
////				if(nodeNumber%100000 == 0)
////					System.out.println("Processed: " + nodeNumber);
//			}
//			System.out.println("total properties = " + wholeProperties);
//			System.out.println("average number of properties per node = " + (double)wholeProperties/nodeNumber);
//			tx.success();
//		}
		
		
		//---------------------------------------------------------
		//This part calculates the number of nodes and relationships.
		//---------------------------------------------------------
		
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			nodeNumber = 0;
//			long maxNodeId = 0;
//		    // Database operations go here
//			System.out.println("...running");
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			for ( Node item : globalOperation.getAllNodes() )
//			 {
//				nodeNumber++;
//				if(item.getId() > maxNodeId)
//					maxNodeId = item.getId();
//			  //   System.out.println(item.getId());
//			 }
//			System.out.println("node number: " + nodeNumber + " , max node id: " + maxNodeId);
//			Iterable<Relationship> neoRelationships = globalOperation.getAllRelationships();
//			for (Relationship item:neoRelationships)
//			{
//				relationshipNumber++;
//			}
//			
//			System.out.println("Relationship number: " + relationshipNumber);
//		    tx.success();
//		}
				
		//---------------------------------------------------------
		//This part calculates the no label Node
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			nodeNumber = 0;
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			int nodeLabelNumber;
//			for (Node n : globalOperation.getAllNodes())
//			{
//				nodeLabelNumber = 0;
//				nodeNumber ++;
//				Iterable<Label> labels = n.getLabels();
//				for(Label label : labels)
//					nodeLabelNumber ++;
//				if(nodeLabelNumber == 0)
//					wholeLabels++;
//			}
//			System.out.println("no label nodes:" + wholeLabels);
//			tx.success();
//		}
		
		//---------------------------------------------------------
		//This part calculates the number of label
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			HashSet<String> labelStringSet = new HashSet<String>();
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			for (Node n : globalOperation.getAllNodes())
//			{
//				Iterable<Label> labels = n.getLabels();
//				for(Label label : labels)
//					labelStringSet.add(label.name());
//			}
//			System.out.println("Number of Labels:" + labelStringSet.size());
//			tx.success();
//		}
		
		//---------------------------------------------------------
		//This part calculates the types of labels
		//---------------------------------------------------------
				
				
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			HashSet<String> labels = new HashSet<String>();
//			nodeNumber = 0;
//			// Database operations go here
//			System.out.println("...running");
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			for ( Node item : globalOperation.getAllNodes() )
//			{
//				for(Label l : item.getLabels())
//					labels.add(l.name());
//			}
//			for(String s : labels)
//				System.out.println(s);
//			System.out.println(labels.size());
//			tx.success();
//		}
			
		//---------------------------------------------------------
		//This part calculates the Average labels per Node
		//---------------------------------------------------------
			
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			nodeNumber = 0;
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			int nodeLabelNumber;
//			for (Node n : globalOperation.getAllNodes())
//			{
//				nodeLabelNumber = 0;
//				nodeNumber ++;
//				Iterable<Label> labels = n.getLabels();
//				for(Label label : labels)
//					nodeLabelNumber ++;
//				wholeLabels += nodeLabelNumber;
////				if(nodeNumber%100000 == 0)
////					System.out.println("Processed: " + nodeNumber);
//			}
//			System.out.println("total labels = " + wholeLabels);
//			System.out.println("average number of labels per node = " + (double)wholeLabels/nodeNumber);
//			tx.success();
//		}
		
		//delete nodes to create a new graph (from some sample nodes)
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			HashSet<Node> nodeSet = new HashSet<Node>();
//			HashSet<Node > nodeSet_1hop = new HashSet<Node>();
//			HashSet<Node > nodeSet_2hop = new HashSet<Node>();
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			for(Node n : globalOperation.getAllNodes())
//			{
//				for(Label l : n.getLabels()){
//					if(l.name().equals("Animal")){
//						nodeSet.add(n);
//					}
//				}
//			}
//			for(Node n : nodeSet){
//				nodeSet_1hop.add(n);
//				if(n.getDegree(Direction.INCOMING) != 0)
//					for(Relationship r : n.getRelationships(Direction.INCOMING))
//						nodeSet_1hop.add(r.getStartNode());
//				if(n.getDegree(Direction.OUTGOING) != 0)
//					for(Relationship r : n.getRelationships(Direction.OUTGOING))
//						nodeSet_1hop.add(r.getEndNode());
//			}
//			for(Node n : nodeSet_1hop){
//				nodeSet_2hop.add(n);
//				for(Relationship r : n.getRelationships(Direction.INCOMING)){
//					nodeSet_2hop.add(r.getStartNode());
//				}
//				for(Relationship r : n.getRelationships(Direction.OUTGOING)){
//					nodeSet_2hop.add(r.getEndNode());
//				}
//
//			}
//			System.out.println();
//			System.out.println("starting to delete");
//			System.out.println("node size 1: " + nodeSet_1hop.size());
//			System.out.println("node size 2: " + nodeSet_2hop.size());
//			nodeSet_2hop.removeAll(nodeSet_1hop);
//			int delCnt=0;
//			for(Relationship r : globalOperation.getAllRelationships())
//			{
//				if(nodeSet_1hop.contains(r.getStartNode()) && nodeSet_1hop.contains(r.getEndNode()))
//					continue;
//				else{ 
//					r.delete();
//					delCnt++;
//					if(delCnt%50000==0)
//						System.out.println(delCnt);
//				}
//			}
//			for(Node n : globalOperation.getAllNodes()){
//				if(!nodeSet_1hop.contains(n))
//					n.delete();
//			}
//			tx.success();
//		}
		
		//---------------------------------------------------------
		//Give 2 node id, output their details and the relationship connect them(if exist)
		//---------------------------------------------------------
		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
		{
			Node n1 = graphneo4j.graphDb.getNodeById(Long.valueOf(args[1]));
			System.out.println("n1:");
			for(Label l : n1.getLabels())
				System.out.println(l);
			for(String s : n1.getPropertyKeys())
				System.out.println(s + "-------" + n1.getProperty(s));
			Node n2 = graphneo4j.graphDb.getNodeById(Long.valueOf(args[2]));
			System.out.println("n2:");
			for(Label l : n2.getLabels())
				System.out.println(l);
			for(String s : n2.getPropertyKeys())
				System.out.println(s + "-------" + n2.getProperty(s));
			boolean n2Exist = false;
			for(Relationship r : n1.getRelationships()){
				if(r.getStartNode().getId() == n2.getId() || r.getEndNode().getId() == n2.getId())
				{
					System.out.println("relationship:");
					n2Exist = true;
					for(String s : r.getPropertyKeys())
						System.out.println(s + "---" + r.getProperty(s));
				}
			}
			if(n2Exist == false)
				System.out.println("no relationship");
			tx.success();
		}
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() ){
//			String inputpathname = args[1];
//			File inputfilename = new File(inputpathname);
//			InputStreamReader reader = new InputStreamReader (new FileInputStream(inputfilename));
//			BufferedReader br = new BufferedReader(reader);
//			
//			String line = "";
//			HashSet<Long> nodeSet = new HashSet<Long>();
//			while (line != null){
//				line = br.readLine();
//				if(line==null)
//					break;
//				nodeSet.add(Long.valueOf(line));
//			}
//			
//			for(Long l1 : nodeSet)
//				for(Long l2 : nodeSet){
//					if(l1 > l2){
//						HashSet<Node> n1set = new HashSet<Node>();
//						HashSet<Node> n2set = new HashSet<Node>();
//						
//						Node n1 = graphneo4j.graphDb.getNodeById(l1);
//						Node n2 = graphneo4j.graphDb.getNodeById(l2);
//						for(Relationship r : n1.getRelationships(Direction.OUTGOING))
//							n1set.add(r.getEndNode());
//						for(Relationship r : n2.getRelationships(Direction.OUTGOING))
//							n2set.add(r.getEndNode());
//						HashSet<Node> n3set = new HashSet<Node>(n1set);
//						n3set.retainAll(n2set);
//						if(n3set.size() != 0)
//						{
//							for(Node n : n3set){
//								boolean aa = false;
//								for(Label l : n.getLabels()){
//									if(l.name().contains("Band"))
//										aa = true;
//								}
//								if(aa == true){
//									System.out.println(l1 + " " + l2 + " " + n.getId() + " " + n.getLabels());
//								}
//							}
//						}
//						n1set.clear();
//						n2set.clear();
//						n3set.clear();
//					}
//				}
//			tx.success();
//		}
		
		//---------------------------------------------------------
		//try to find those kind of node pairs(A-B), which they have bi-relations (A->B and B->A)
		//---------------------------------------------------------
		
//		try ( Transaction tx = graphneo4j.graphDb.beginTx() )
//		{
//			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphneo4j.graphDb);
//			for ( Node item : globalOperation.getAllNodes() )
//			 {
//				for (Relationship r : item.getRelationships(Direction.OUTGOING)){
//					for(Relationship r1 : r.getEndNode().getRelationships(Direction.OUTGOING))
//						if(r1.getEndNode().getId() == item.getId())
//							System.out.println("Node |" + r.getEndNode().getLabels() + "| to node |" + r1.getEndNode().getLabels() + "| " + "id: " + r.getEndNode().getId() + " " + r1.getEndNode().getId());
//				}
//			 }
//		    tx.success();
//		}
		
		
		
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
