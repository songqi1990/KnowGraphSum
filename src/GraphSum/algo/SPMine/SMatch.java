package wsu.eecs.mlkd.KGQuery.algo.SPMine;

import scala.collection.mutable.HashTable;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.AVpair;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Label_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Relationship_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_joinsim;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.IncJoinSimulation;
import wsu.eecs.mlkd.KGQuery.algo.SPMine.MinHash;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeMap;
import java.util.Vector;

import org.neo4j.graphdb.Transaction;


public class SMatch {
	public HashSet<gpm_graph> divSP;	//current diversified summary pattern
	public gpm_graph queryGraph;		//query graph
	public Hashtable<Integer,Integer> weightTable;		//store weight for each pattern
	public Vector<gpm_edge> S;
	public HashSet<gpm_edge> Ec;		//store already covered pattern edges in Q
	public Vector<gpm_graph> PResult;	//result
	public Vector<gpm_graph> candidateP;	//store those pattern which can cover the query potentially 
	public Hashtable<Integer, Vector<gpm_edge>> simResult; //store match edge for each pattern
	public Hashtable<String, Boolean> nodeTag;	//record if a node is covered by the views
	public SMatch(){
		divSP = new HashSet<gpm_graph>();
		queryGraph = new gpm_graph();
		weightTable = new Hashtable<Integer,Integer>();
		S = new Vector<gpm_edge>();
		Ec = new HashSet<gpm_edge>();
		PResult = new Vector<gpm_graph>();
		candidateP = new Vector<gpm_graph>();
		simResult = new Hashtable<Integer,Vector<gpm_edge>>();
		nodeTag = new Hashtable<String, Boolean>();
	}
	
	public void ReadTopK() throws IOException{
		String filename = "/Users/qsong/Downloads/v1";
		FileInputStream inputFile = new FileInputStream(filename);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				inputFile));
		String strLine="";
		Vector<String> vec=null;
		strLine = in.readLine();
		int numberPatterns = Integer.valueOf(strLine);		//read the number of all patterns
		
		for(int i=1; i<=numberPatterns;i++){
			gpm_graph g = new gpm_graph();
			g.simMatchSet = new TreeMap<String, HashSet<String>>();
			String ntag = "";
			String nLabel = "";
			double nweight = 0.0;
			Vector<AVpair> alist = new Vector<AVpair>();
			
			strLine = in.readLine();
			g.graphId = Integer.valueOf(strLine);		//read graphId
			
			strLine = in.readLine();
			int nodeNumber = Integer.valueOf(strLine);	//read node number
			
			for(int j = 0; j < nodeNumber; j++){		//read node info
				strLine=in.readLine().trim();
				vec = createVectorFromString(strLine,"	");
				ntag = vec.elementAt(0);
				nLabel = vec.elementAt(1);
				alist.clear();
				gpm_node v = new gpm_node(alist,ntag,nweight,nLabel);
				g.addVertex(v);
			}
			
			strLine = in.readLine();
			int edgeNumber = Integer.valueOf(strLine);	//read edge number
			
			for(int j = 0; j < edgeNumber; j++){
				strLine = in.readLine().trim();
				vec = createVectorFromString(strLine,"	");
				gpm_edge e = new gpm_edge(vec.elementAt(0),vec.elementAt(1),Integer.parseInt(vec.elementAt(2)), Integer.parseInt(vec.elementAt(3)));
				g.InsertEdge(e);
			}
			for(int k = 0; k < g.vertexSet().size(); k++){
				strLine = in.readLine();
				String key = strLine;
				strLine = in.readLine();
				int number = Integer.valueOf(strLine);
				HashSet<String> matchS = new HashSet<String>();
				for(int p = 0; p < number; p++){
					strLine = in.readLine();
					matchS.add(strLine);
				}
				g.simMatchSet.put(key, matchS);
			}
			g.gfilename = String.valueOf(g.graphId) + "." + "grh";
			int weight = 0;
			for(String s : g.simMatchSet.keySet())
				weight += g.simMatchSet.get(s).size();
			weightTable.put(g.graphId, weight);
			this.divSP.add(g);
		}	
	}
	
	void ReadQuery() throws IOException{
		String filename = "/Users/qsong/Downloads/query1";
		FileInputStream inputFile = new FileInputStream(filename);
		BufferedReader in = new BufferedReader(new InputStreamReader(
				inputFile));
		
		String ntag = "";
		String nLabel = "";
		double nweight = 0.0;
		Vector<AVpair> alist = new Vector<AVpair>();
		
		String strLine="";
		Vector<String> vec=null;
		strLine = in.readLine();		//read node number
		
		int nodeNumber = Integer.valueOf(strLine);	//read node number
		
		for(int j = 0; j < nodeNumber; j++){		//read node info
			strLine=in.readLine().trim();
			vec = createVectorFromString(strLine,"	");
			ntag = vec.elementAt(0);
			nLabel = vec.elementAt(1);
			alist.clear();
			gpm_node v = new gpm_node(alist,ntag,nweight,nLabel);
			this.queryGraph.addVertex(v);
		}
		
		strLine = in.readLine();
		int edgeNumber = Integer.valueOf(strLine);	//read edge number
		
		for(int j = 0; j < edgeNumber; j++){
			strLine = in.readLine().trim();
			vec = createVectorFromString(strLine,"	");
			gpm_edge e = new gpm_edge(vec.elementAt(0),vec.elementAt(1),Integer.parseInt(vec.elementAt(2)), Integer.parseInt(vec.elementAt(3)));
			this.queryGraph.InsertEdge(e);
		}
		this.queryGraph.graphId = 0;
		this.queryGraph.gfilename = "query.grh";
		for(gpm_node n : this.queryGraph.vertexSet()){
			nodeTag.put(n.tag, false);
		}
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
	
	//	============================================================================//
	//	This function displays topk view set
	//	============================================================================//
	public void displayTopKPatternFinal(HashSet<gpm_graph> patternSet){

		for(gpm_graph g : patternSet){
			System.out.println("Graph: " + g.graphId);
			for(gpm_node n : g.vertexSet()){
				System.out.println(n.tag+": "+n.nlabel);
			}
			for(gpm_edge e : g.edgeSet()){
				System.out.println(e.from_node + "-----" + e.to_node);
			}
		}
	}
	
	//	============================================================================//
	//	for pattern selection 
	//	============================================================================//
	public void Selection() throws IOException{
		for(gpm_graph g : divSP){
			gpm_joinsim sim = new gpm_joinsim(g, this.queryGraph);
			Vector<gpm_edge> gMatch = sim.IsBSim(true);
			if(gMatch!=null){
				S.addAll(gMatch);
				candidateP.add(g);
				simResult.put(g.graphId, gMatch);
			}
		}
		while(S.size() != 0){		
			gpm_graph currentP = new gpm_graph();
			Integer minWeight = Integer.MAX_VALUE;
			for(gpm_graph g : candidateP){
					if(weightTable.get(g.graphId) < minWeight){
						minWeight = weightTable.get(g.graphId);
						currentP = g;
					}
			}
			Vector<gpm_edge> Pi_Q = simResult.get(currentP.graphId);
			Pi_Q.removeAll(Ec);
			if(Pi_Q.size() != 0){
				S.removeAll(simResult.get(currentP.graphId));
				Ec.addAll(simResult.get(currentP.graphId));
				PResult.add(currentP);
				candidateP.remove(currentP);
				for(gpm_edge e : simResult.get(currentP.graphId)){
					nodeTag.put(e.from_node, true);
					nodeTag.put(e.to_node, true);
				}
				if(Ec.size() == this.queryGraph.edgeSet().size()){
					return;
				}
			}
		}
	}
	public static void main(String[] args) throws IOException {
		SMatch ps = new SMatch();
		ps.ReadTopK();
//		ps.displayTopKPatternFinal(ps.divSP);
		//==============================================//
		//this part is transform a subgraph induced by a pattern into a graph file for VF2
		//here we need a continuous node list.
		//==============================================//
//		HashMap<String,String> IdMatch = new HashMap<String,String>();
//		String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup";
//		Neo4jGraph dg = new Neo4jGraph(filename,2);
//		String outfilename = "/Users/qsong/Downloads/g1graph";
//		FileWriter fw = new FileWriter(outfilename);
//		PrintWriter pw = new PrintWriter(fw);
//		int nodeI = 0;
//		try(Transaction tx1 = dg.getGDB().beginTx() ){
//		for(gpm_graph g : ps.divSP){
//			for(gpm_node n : g.vertexSet()){
//				for(String mN : g.simMatchSet.get(n.tag))
//					if(!IdMatch.containsKey(mN)){
//						IdMatch.put(mN, String.valueOf(nodeI));
//						if(n.nlabel.equals("wikicat_Living_people"))
//							pw.println("v " + IdMatch.get(mN) + " 73");
//						if(n.nlabel.equals("wordnet_site_108651247"))
//							pw.println("v " + IdMatch.get(mN) + " 423");
//						if(n.nlabel.equals("wikicat_The_Football_League_players"))
//							pw.println("v " + IdMatch.get(mN) + " 236");
//						if(n.nlabel.equals("wikicat_English_footballers"))
//							pw.println("v " + IdMatch.get(mN) + " 29");
//						if(n.nlabel.equals("wordnet_club_108227214"))
//							pw.println("v " + IdMatch.get(mN) + " 302");
//						nodeI++;
//				}
//			}
//			for(gpm_edge e : g.edgeSet()){
//				for(String g1 : g.simMatchSet.get(e.from_node))
//					for(String g2 : g.simMatchSet.get(e.to_node)){
//						if(dg.getRelationship(Long.valueOf(g1), Long.valueOf(g2))!=null)
//							pw.println("e " + IdMatch.get(g1) + " " + IdMatch.get(g2));
//					}
//			}
//		}
//		tx1.success();
//		pw.close();
//		fw.close();
//		}
				
		ps.ReadQuery();	
		ps.Selection();
		if(ps.PResult.size()!=0){
			for(gpm_graph g : ps.PResult)
				System.out.println(g.graphId);
		}
		System.out.println(ps.nodeTag);
	}
}
