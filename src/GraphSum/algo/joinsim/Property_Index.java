package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Vector;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;


public class Property_Index {
	public String propertyidxFilename;
	public Neo4jGraph G;
	public Hashtable<String, HashMap> proIndex;
	
	public Property_Index(){
	}
	
	@SuppressWarnings("rawtypes")
	public Property_Index(Neo4jGraph g){
		G = g;
		proIndex = new Hashtable<String,HashMap>();
		String gname = this.G.gfilename;
		propertyidxFilename = gname.substring(0, gname.indexOf("."))+"_Property_Idx.txt";
	}
	
	//build inverted index
	//if file already exists, restore the index from the file;
	//otherwise, recompute.
	public void bulidPropertyIndex(boolean store) throws IOException{
		
		//if index has been stored
		File idxfile = new File(propertyidxFilename);
		if(idxfile.exists()){
			System.out.println("Property index file exists");
			constructPropertyIdxFromFile();
			//DisplayPropertyIdx();
			return;
		}
		
		System.out.println("Property index file not exists. Rebuild.");
		for (Node n : G.nodes)
			onPropertyInsVertex(n);
		
		if(store){
			System.out.println("Storing Property Idx");
			this.PropertyStore();
		}
		//DisplayPropertyIdx();
	}
	
	//This function incrementally update index upon new insertions of vertex
	@SuppressWarnings("unchecked")
	public void onPropertyInsVertex(Node n){
		for (String a : n.getPropertyKeys()){
			HashMap values = proIndex.get(a);
			if(values == null){
				values = new HashMap<String,HashSet<String>>();
				proIndex.put(a, values);
			}
			//System.out.println(a);
			//System.out.println(n.getProperty(a));
			HashSet<String> nset2 = (HashSet<String>) values.get(n.getProperty(a).toString());
			if(nset2 == null)
				nset2 = new HashSet<String>();
			nset2.add(String.valueOf(n.getId()));
			values.put(n.getProperty(a), nset2);
			proIndex.put(a, values);
		}
	}
	
	@SuppressWarnings("unchecked")
	public void PropertyStore() throws IOException{
		FileWriter fw = new FileWriter(propertyidxFilename);
		PrintWriter pw = new PrintWriter(fw);
		
		pw.println(this.proIndex.keySet().size());
		
		//for each attributes: values;
		for(String s : this.proIndex.keySet()){
			pw.println(s + "	" + this.proIndex.get(s).keySet().size());
			//for each value: node id with corresponding value
			
			for (Object value : this.proIndex.get(s).keySet()){
				pw.print(value.toString() + "	");
				for (String tag : (HashSet<String>)this.proIndex.get(s).get(value)){
					pw.print(tag + "	");
				}
				pw.println();
			}
			
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
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(words[i]);
		}
		return vec;
	}
	

	@SuppressWarnings("unchecked")
	public void constructPropertyIdxFromFile() throws IOException{
		BufferedReader in = MyFileOperation.openFile(propertyidxFilename);
		String strLine="";
		Vector<String> vec=null;
		
		//line 0: property number
		strLine = in.readLine().trim();
		int attnum = Integer.parseInt(strLine);
		 
		if (proIndex == null)
			proIndex = new Hashtable<String, HashMap>();
		
		//construct corresponding treemaps
		for (int i = 0; i < attnum; i++){
			strLine = in.readLine().trim();
			//attribute name: value num pair
			vec = createVectorFromString(strLine,"	");
			String attname = vec.elementAt(0);
			int vnum = Integer.parseInt(vec.elementAt(1));
			System.out.println(attname + ": " + vnum);
			HashMap vnids = new HashMap<String,HashSet<String>>();
			for (int j = 0; j < vnum; j++){
				//value: nid set pair
				strLine = in.readLine().trim();
				vec = createVectorFromString(strLine, "	");
				String value = vec.elementAt(0);
				HashSet<String> nidset = new HashSet<String>();
				for (int k = 1; k < vec.size(); k++)
					nidset.add(vec.elementAt(k));
				vnids.put(value, nidset);
			}
			proIndex.put(attname, vnids);
		}
	}
	
	public HashSet<String> GetAnswer(HashSet<String> keys){
		HashSet<String> res = new HashSet<String>();
		for (String key : keys){
			HashSet<String> keyRes = new HashSet<String>();
			for (Object value : this.proIndex.get(key).keySet()){
				HashSet<String> tagRes = (HashSet<String>)this.proIndex.get(key).get(String.valueOf(value));
				keyRes.addAll(tagRes);
			}	
			System.out.println(key + ": " + keyRes.size());
			if(res.size() == 0)
				res.addAll(keyRes);
			else
				res.retainAll(keyRes);
		}
		return res;
	}
	
	//display
	@SuppressWarnings("unchecked")
	public void DisplayPropertyIdx(){
		for(String s : this.proIndex.keySet()){
			System.out.println(s + this.proIndex.get(s).size());
			/*
			for(Object value : this.proIndex.get(s).keySet()){
				System.out.print(value.toString() + "	");
				for(String tag : (HashSet<String>)this.proIndex.get(s).get(value)){
					System.out.print(tag + "	");
				}
				System.out.println();
			}
			*/
		}
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		Neo4jGraph G = new Neo4jGraph("/Users/qsong/Downloads/YagoCores_graph.db",2);
		HashSet<String> input = new HashSet<String>();
		Property_Index idx = new Property_Index(G);
		input.add("hasGivenName");
		input.add("hasFamilyName");
		try(Transaction tx = G.getGDB().beginTx()){
		idx.bulidPropertyIndex(true);
		tx.success();
		}
		long st1 = System.currentTimeMillis();
		System.out.println("final: " + idx.GetAnswer(input).size());
		long st2 = System.currentTimeMillis();
		System.out.println("Time spend " + (st2 - st1));
	}
}
