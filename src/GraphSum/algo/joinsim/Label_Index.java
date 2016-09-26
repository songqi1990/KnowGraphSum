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

public class Label_Index {

	public String labelidxFilename;
	public Neo4jGraph G;
	public Hashtable<String,HashSet> invIndex;
	
	public Label_Index(){
	}
	
	public Label_Index(Neo4jGraph g){
		G = g;
		invIndex = new Hashtable<String, HashSet>();
		String gname = this.G.gfilename;
		labelidxFilename = gname.substring(0, gname.indexOf("."))+"_Label_Idx.txt";
	}
	
	//build inverted index for label
	//if file already exists, restore the index from the file
	//otherwise, recompute
	//!!!restore label index from file is much slower than re-compute
	/*public void buildLabelIndex(boolean store) throws IOException{
		
		//if index has been stored.
		File idxfile = new File(labelidxFilename);
		
		if (idxfile.exists()){
			System.out.println("Label index file exists.");
			constructLabelIdxFromFile();
			//DisplayIdx();
			return;
		}
		
		System.out.println("Label index file not exists. Rebuild");
		for (Node n : G.nodes){
			onLabelInsVertex(n);
		}
		
		if(store){
			System.out.println("Storing Label Index");
			this.LabelStore();
		}
		//DisplayIdx();
	}
	*/
	
	public void buildLabelIndex(boolean store) throws IOException{
		System.out.println("Build label index");
		for (Node n : G.nodes){
			onLabelInsVertex(n);
		}
		this.LabelStore();
		System.out.println(this.invIndex.keySet().size());
	}
	
	
	public Hashtable<String, Integer> GetNodeLabelCount() throws IOException{
		Hashtable<String, Integer> r = new Hashtable<String, Integer>();
		for(String key : invIndex.keySet())
			r.put(key, invIndex.get(key).size());
		//for(String k : r.keySet())
			//System.out.println(k + r.get(k));
		return r;		
	}
	
	
	//This function incrementally update index upon new insertions of vertex
	public void onLabelInsVertex(Node n){
		for (Label a : n.getLabels()){
			HashSet values = invIndex.get(a.name());
			if(values == null){
				values = new HashSet<String>();
				invIndex.put(a.name(), values);
			}	
			values.add(String.valueOf(n.getId()));
			invIndex.put(a.name(), values);
		}
	}
	
	//store
	public void LabelStore() throws IOException{
		
		FileWriter fw = new FileWriter(labelidxFilename);
		PrintWriter pw = new PrintWriter(fw);
		
		pw.println(this.invIndex.keySet().size());
		//for each labels
		for(String s : this.invIndex.keySet()){
			//if(this.invIndex.get(s).size() < 10000){
			pw.println(s + "\t"+ this.invIndex.get(s).size());
//			if(this.invIndex.get(s).size() > 10000)
//				System.out.println(s + "	"+ this.invIndex.get(s).size());
			
//			for(String tag : (HashSet<String>)this.invIndex.get(s)){
//				pw.print(tag + "	");
//			}
//			pw.println();
//			//}
		}
		pw.close();
		fw.close();
	}
	
	//construct index from the file.
	public void constructLabelIdxFromFile() throws IOException{
		BufferedReader in = MyFileOperation.openFile(labelidxFilename);
		String strLine = "";
		Vector<String> vec = null;
		
		//line 0: label number
		strLine = in.readLine().trim();
		int labelnum = Integer.parseInt(strLine);
		
		//construct corresponding labels
		for (int i = 0; i < labelnum; i++){
			strLine = in.readLine().trim();
			vec = createVectorFromString(strLine, "	");
			String labelName = vec.elementAt(0);
			int vnum = Integer.parseInt(vec.elementAt(1));
			System.out.println(labelName + ": " +vnum);
			HashSet vnids = new HashSet<String>();
			strLine = in.readLine().trim();
			for(int j = 0; j < vnum; j++){
				vec = createVectorFromString(strLine,"	");
				for (int k = 0; k < vec.size(); k++){
					vnids.add(vec.elementAt(k));
				}
			}
			invIndex.put(labelName, vnids);
		}
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
	
	//display
	public void DisplayLabelIdx(){
		for (String s : this.invIndex.keySet()){
			System.out.println(s + this.invIndex.get(s).size());
			/*
			for (Object value : this.invIndex.get(s)){
				System.out.print(value + "	");
			}
			*/
			System.out.println();
		}
	}
	
	@SuppressWarnings("unchecked")
	public HashSet<String> getLabelAnswer(Vector<Label> labels){
		HashSet<String> ans = new HashSet<String>();
		for(Label l : labels){
			ans.addAll(this.invIndex.get(l.name()));
		}
		return ans;
	}
	
	@SuppressWarnings("unchecked")
	public HashSet<String> getLabelAnswer(String label){
		return this.invIndex.get(label);
	}
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException{
		Vector<Label> labels = new Vector<Label>();
		HashSet<String> input = new HashSet<String>();
		HashSet<String> testans = new HashSet<String>();
		Neo4jGraph G = new Neo4jGraph("/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup",2);
		//input.add("wikicat_American_television_actors");
		try(Transaction tx = G.getGDB().beginTx()){
		Label_Index idx = new Label_Index(G);
		idx.buildLabelIndex(true);
		idx.GetNodeLabelCount();
		System.out.println("Index build complete");
		//testans =idx.GetAnswer(input);
		//System.out.println(testans.size());
		tx.success();
		}
	}
}
