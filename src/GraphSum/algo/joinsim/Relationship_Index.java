package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.Vector;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class Relationship_Index {
	public String relidxFilename;
	public Neo4jGraph G;
	public Hashtable<String,HashSet> relIndex;
	
	public Relationship_Index(){
		
	}
	
	public Relationship_Index(Neo4jGraph g){
		G = g;
		relIndex = new Hashtable<String,HashSet>();
		String gname = this.G.gfilename;
		relidxFilename = gname.substring(0, gname.indexOf("."))+"_Relationship_Idx.txt";
	}
	
	//build iverted index for relationship
	//if file already exists, restore the index from the file
	//otherwise, recompute
	public void buildRelIndex(boolean store) throws IOException{
		System.out.println("Build relationship index");
		for (Relationship r : G.edges){
			onRelInsVertex(r);
		}
		RelStore();
	}
	
	public Hashtable<String, Integer> GetEdgeLabelCount() throws IOException{
		Hashtable<String, Integer> r = new Hashtable<String, Integer>();
		for(String key : relIndex.keySet())
			r.put(key, relIndex.get(key).size());
//		for(String k : r.keySet())
//			System.out.println(k + ": " + r.get(k));
		return r;		
	}
	
	//This function incrementally update index upon new insertions of vertex
	public void onRelInsVertex(Relationship r){
		for (String propertykey : r.getPropertyKeys()){
			HashSet values = relIndex.get(r.getProperty(propertykey).toString());
			if(values == null){
				values = new HashSet<String>();
				relIndex.put(r.getProperty(propertykey).toString(), values);
			}
			values.add(String.valueOf(r.getId()));
			relIndex.put(r.getProperty(propertykey).toString(), values);
		}
	}
	
	//store
	public void RelStore() throws IOException{
		FileWriter fw = new FileWriter(relidxFilename);
		PrintWriter pw = new PrintWriter(fw);
		
		pw.println(this.relIndex.keySet().size());
		//for each relationship properties
		for(String s : this.relIndex.keySet()){
			pw.println(s + "	" + this.relIndex.get(s).size());
			//for (String tag : (HashSet<String>)this.relIndex.get(s)){
				//pw.print(tag + "	");
			//}
			//pw.println();
		}
		pw.close();
		fw.close();
	}
	
	//	============================================================================//
	//	This function creates a vector from a string
	//	============================================================================//
	public Vector<String> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<String> vec = new Vector<String>();
		//String[] words = strContent.split(strDelimiter);
		StringTokenizer token = new StringTokenizer(strContent,strDelimiter);
//		for (int i = 0; i < words.length; i++) 
//			vec.addElement(words[i]);
		while(token.hasMoreElements())
			vec.add(token.nextToken());
		return vec;
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		Vector<Label> labels = new Vector<Label>();
		HashSet<String> testans = new HashSet<String>();
		Neo4jGraph G = new Neo4jGraph(args[0],2);
		
		long st1 = System.currentTimeMillis();
		try(Transaction tx = G.getGDB().beginTx()){
		Relationship_Index idx = new Relationship_Index(G);
		idx.buildRelIndex(true);
		idx.GetEdgeLabelCount();
		System.out.println("Index build complete");
		/*
		for(int i = 1; i<10; i++){
			for (Label l : G.getGDB().getNodeById((long)i).getLabels())
			labels.add(l);
		}
		System.out.println(labels);
		testans = idx.getLabelAnswer(labels);
		System.out.println(testans.size());
		
		*/
		tx.success();
		}
		long st2 = System.currentTimeMillis();
		System.out.println("Time spend " + (st2 - st1));
	}
}
