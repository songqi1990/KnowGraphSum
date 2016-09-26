package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.Vector;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;

public class Property_Value_Index {

	public String propertyvalueidxFilename;
	public Neo4jGraph G;
	public Hashtable<String,HashSet> provalueIndex;
	
	public Property_Value_Index(){
	}
	
	public Property_Value_Index(Neo4jGraph g){
		G = g;
		provalueIndex = new Hashtable<String, HashSet>();
		String gname = this.G.gfilename;
		propertyvalueidxFilename = gname.substring(0, gname.indexOf("."))+"_Property_Value_Idx.txt";
	}
	
	//build inverted index for label
	//if file already exists, restore the index from the file
	//otherwise, recompute
	public void buildPropertyValueIndex(boolean store) throws IOException{
		
		//if index has been stored.
		File idxfile = new File(propertyvalueidxFilename);
		if (idxfile.exists()){
			System.out.println("Label index file exists.");
			//constructLabelIdxFromFile();
			//DisplayIdx();
			return;
		}
		
		System.out.println("Label index file not exists. Rebuild");
		for (Node n : G.nodes){
			onProValueInsVertex(n);
		}
		
		if(store){
			System.out.println("Storing Label Index");
			this.ProValueStore();
		}
		//DisplayIdx();
	}
	
	//This function incrementally update index upon new insertions of vertex
	public void onProValueInsVertex(Node n){
		for (String a : n.getPropertyKeys()){
			HashSet values = provalueIndex.get(n.getProperty(a).toString());
			if(values == null){
				values = new HashSet<String>();
				provalueIndex.put(n.getProperty(a).toString(), values);
			}
			
			values.add(String.valueOf(n.getId()));
			provalueIndex.put(n.getProperty(a).toString(), values);
		}
	}
	
	//store
	public void ProValueStore() throws IOException{
		int number = 0;
		FileWriter fw = new FileWriter(propertyvalueidxFilename);
		PrintWriter pw = new PrintWriter(fw);
		
		pw.println(this.provalueIndex.keySet().size());
		//for each labels
		for(String s : this.provalueIndex.keySet()){
			//if(this.provalueIndex.get(s).size() > 100)
			pw.println(s + "	"+ this.provalueIndex.get(s).size());
			for(String tag : (HashSet<String>)this.provalueIndex.get(s)){
				pw.print(tag + "	");
			}
			pw.println();
			
		}
		//System.out.println("number: " + number);
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
		Neo4jGraph G = new Neo4jGraph("/Users/qsong/Downloads/dbpedia_infobox_properties_en.db",2);
		
		long st1 = System.currentTimeMillis();
		try(Transaction tx = G.getGDB().beginTx()){
			Property_Value_Index idx = new Property_Value_Index(G);
		idx.buildPropertyValueIndex(true);
		System.out.println("Index build complete");
		/*
		for(int i = 1; i<10; i++){
			for (Label l : G.getGDB().getNodeById((long)i).getLabels())
			labels.add(l);
		}
		System.out.println(labels);
		testans = idx.getLabelAnswer(labels);
		System.out.println(testans.size());
		tx.success();
		*/
		}
		long st2 = System.currentTimeMillis();
		System.out.println("Time spend " + (st2 - st1));
	}
}
