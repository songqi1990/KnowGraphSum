package wsu.eecs.mlkd.KGQuery.algo.SPMine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.AVpair;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Label_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Relationship_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.IncJoinSimulation;
import wsu.eecs.mlkd.KGQuery.algo.SPMine.MinHash;

public class two_appro_basic {
	public static Neo4jGraph dg;
	
	public Label_Index labelIndex;
	//public Hashtable<Integer, HashSet> frePattern;	//used to store patterns generated from each iteration
	public String[] edgeStoreStartLabel;//store edge, each Label pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEndLabel;
	public String[] edgeStoreStart;//store edge, each related node pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEnd;
	public HashSet<divSP_pair> divSP;	//current diversified summary pattern
//	public HashSet<divSP_pair> divSP1;
//	public HashSet<divSP_pair> divSP2;
//	public HashSet<divSP_pair> divSP3;
//	public HashSet<divSP_pair> divSP4;
	public gpm_graph lastPat;
	public HashSet<gpm_graph> PminustopK;
	public HashSet<gpm_graph> P;
	public HashSet<gpm_graph> divSPInit;
	public boolean isExistPat;
	public Relationship_Index relIndex;
	public HashSet<gpm_graph> frePatternI[];
	public int existingEdgeAdd;
	public Hashtable<String, Integer> nodeLabelCount;
	public Hashtable<String, Integer> edgeLabelCount;
	public int dataGraphSize;
	public int globalCounter; //count for debug: stores the number of combine
	
	public int couverageRange;	//range of couverage for each pattern
	public int graphCounter;	//A pattern Id for each pattern
	public File outputfile;
//	public FileWriter fw;
//	public PrintWriter pw;
	public MinHash minhash;
	HashSet<String> aaa;
	HashSet<String> bbb;
	
	public two_appro_basic(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		relIndex = new Relationship_Index(G);
		//frePattern = new Hashtable<Integer,HashSet>();
		frePatternI = new HashSet[10];
		for(int i =0 ;i < 10 ;i ++){
			frePatternI[i] = new HashSet<gpm_graph>();
			//frePattern.put(i, frePatternI[i]);
		}
		divSP = new HashSet<divSP_pair>();
		divSPInit = new HashSet<gpm_graph>();
//		
		PminustopK = new HashSet<gpm_graph>();
		P = new HashSet<gpm_graph>();

//		divSP1 = new HashSet<divSP_pair>();
//		divSP2 = new HashSet<divSP_pair>();
//		divSP3 = new HashSet<divSP_pair>();
//		divSP4 = new HashSet<divSP_pair>();
		
		edgeStoreStart = new String[999999];
		edgeStoreEnd = new String[999999];
		edgeStoreStartLabel = new String[999999];
		edgeStoreEndLabel = new String[999999];
		nodeLabelCount = new Hashtable<String, Integer>();
		edgeLabelCount = new Hashtable<String, Integer>();
		graphCounter = 1;
		globalCounter = 0;
		existingEdgeAdd = 0;
		isExistPat = false;
		
		dataGraphSize = dg.getNodeNumber() + dg.getEdgeNumber();
	
		minhash = new MinHash(0.01, 10000000);
		aaa = new HashSet<String>();

//		outputfile = new File("/home/qsong/program/SPMine/output.txt");
//		fw = new FileWriter("/home/qsong/program/SPMine/output.txt");
//		outputfile = new File("/Users/qsong/Downloads/output.txt");
//		fw = new FileWriter("/Users/qsong/Downloads/output.txt");
//		pw = new PrintWriter(fw);
	}
	
	//find candidate edge set for iteration 1 based on frequency of edge label and node label
	public boolean Init() throws IOException{	//generate level 0 frequent edges
		
		BuildLabelIdx();
		BuildRelationshipIdx();
		nodeLabelCount = labelIndex.GetNodeLabelCount();
		edgeLabelCount = relIndex.GetEdgeLabelCount();
		gpm_graph newGraph = new gpm_graph();
		for(Relationship r : dg.edges){
			int edgeFre = 0;
			boolean flag = true;
			
			for(String k : r.getPropertyKeys()){
				edgeFre = (int)edgeLabelCount.get(r.getProperty(k).toString());
				if(CheckFrequency(edgeFre, 100, 90000000)){
					flag = true;
					break;
					}
			}
			
			if(flag == true){
				Node startNode = r.getStartNode();
				Node endNode = r.getEndNode();
				if(!CheckLabel(startNode) || !CheckLabel(endNode))
					continue;
				CalculateMaxLabel(startNode);
				CalculateMaxLabel(endNode);
				int startNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(startNode.getId()));
				int endNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(endNode.getId()));
				if(CheckFrequency(startNodeFrequency,100, 90000000) && CheckFrequency(endNodeFrequency, 100, 90000000)){
					if(CheckEdgeDuplicate(r, dg.maxLabelofNode.get(startNode.getId()),dg.maxLabelofNode.get(endNode.getId()))){
						newGraph = AddFreEdgetoGraph(r);
						frePatternI[1].add(newGraph);
						edgeStoreStartLabel[graphCounter] = dg.maxLabelofNode.get(startNode.getId());
						edgeStoreEndLabel[graphCounter] = dg.maxLabelofNode.get(endNode.getId());
						edgeStoreStart[graphCounter] = String.valueOf(startNode.getId());
						edgeStoreEnd[graphCounter] = String.valueOf(endNode.getId());
						graphCounter ++;
					}
				}
			}
		}
		System.out.println("number of frequent patterns in iteration 1: " + frePatternI[1].size());
		return true;
	}
	
	//check if this edge already exists in the frequent edge set
	//if not exist return true
	//if already exist, add it to existing matching set
	public boolean CheckEdgeDuplicate(Relationship re, String label1, String label2){
		for(int i = 1; i < graphCounter; i++)
			if(edgeStoreStartLabel[i].equals(label1) && edgeStoreEndLabel[i].equals(label2)){
				for(gpm_graph g : frePatternI[1]){
					if(g.graphId == i){
						existingEdgeAdd ++;
						if(!g.simMatchSet.get(edgeStoreStart[i]).contains(String.valueOf(re.getStartNode().getId())))
							g.simMatchSet.get(edgeStoreStart[i]).add(String.valueOf(re.getStartNode().getId()));
						if(!g.simMatchSet.get(edgeStoreEnd[i]).contains(String.valueOf(re.getEndNode().getId())))
							g.simMatchSet.get(edgeStoreEnd[i]).add(String.valueOf(re.getEndNode().getId()));
					}
				}
				return false;
			}
		return true;
	}
	
	//We do not consider those nodes who do not have labels
	public boolean CheckLabel(Node n){
		Iterable<Label> lset = n.getLabels();
		if(lset.iterator().hasNext())
			return true;
		else
			return false;
	}
	
	//add this new frequent edge into the graph
	public gpm_graph AddFreEdgetoGraph(Relationship r){
		gpm_graph g = new gpm_graph();
		String ntag1 = "";
		String ntag2 = "";
		double nweight = 0.0;
		String nlabel1 = "";
		String nlabel2 = "";
		Vector<AVpair> alist = new Vector<AVpair>();
		g.simMatchSet = new TreeMap<String, HashSet<String>>();
		HashSet<String> nodeMatch1 = new HashSet<String>();
		HashSet<String> nodeMatch2 = new HashSet<String>();
		
		alist.clear();
		ntag1 = Long.toString(r.getStartNode().getId());
		nweight = 0.0;
		nlabel1 = dg.maxLabelofNode.get(r.getStartNode().getId());
		gpm_node startNode = new gpm_node(alist,ntag1,nweight,nlabel1);	
		g.addVertex(startNode);
		nodeMatch1.add(Long.toString(r.getStartNode().getId()));
		
		alist.clear();
		ntag2 = Long.toString(r.getEndNode().getId());
		nweight = 0.0;
		nlabel2 = dg.maxLabelofNode.get(r.getEndNode().getId());
		gpm_node endNode = new gpm_node(alist,ntag2,nweight,nlabel2);	
		g.addVertex(endNode);
		nodeMatch2.add(Long.toString(r.getEndNode().getId()));
		
		gpm_edge e = new gpm_edge(startNode.tag,endNode.tag,1,0);
		g.InsertEdge(e);
		/*
		for(String s1 : labelIndex.getLabelAnswer(nlabel1))
			for(String s2 : labelIndex.getLabelAnswer(nlabel2)){
				if(dg.containsEdge(dg.GetVertex(Long.valueOf(s1)), dg.GetVertex(Long.valueOf(s2)))){
					nodeMatch1.add(s1);
					nodeMatch2.add(s2);
				}
			}
		*/
		g.edgeSetMark = new TreeSet<Integer>();
		g.edgeSetMark.add(graphCounter);
		g.hashResult = new int[10000];
		for(int i = 0; i < 10000; i ++)
			g.hashResult[i] = 0;
		g.graphId = graphCounter;
		g.simMatchSet.put(ntag1, nodeMatch1);
		g.simMatchSet.put(ntag2, nodeMatch2);
		
		return g;
	}
	
	public boolean CheckFrequency(int fre, int low, int upper){
		if(fre >= low && fre <= upper)
			return true;
		else 
			return false;
	}
	
	
	//calculate max label for each node and put the result back to maxLabelofNode of Neo4jGraph
	public void CalculateMaxLabel(Node a){
		String maxLabel = "";
		int maxLabelFrequency = 0;
		if(dg.maxLabelofNode.containsKey(a.getId()))
			return;
		else{
			for(Label l: a.getLabels()){
				if(nodeLabelCount.get(l.toString()) > maxLabelFrequency){
					maxLabel = l.toString();
					maxLabelFrequency = nodeLabelCount.get(maxLabel);	
				}
			}
			dg.maxLabelofNode.put(a.getId(), maxLabel);
		}
	}
	
	//main function of SPMine
	@SuppressWarnings("unchecked")
	public void PatternMining(Neo4jGraph dg, int patternSize, double theta, double alpha, int resultSize) throws IOException{
		
		for(int i = 2 ; i < patternSize; ){
			if( i > 1)
				if(frePatternI[i-1].size() == 0){
					System.out.println("Pattern mining stop");
					return;
				 }
			globalCounter = 0;
			//HashSet<gpm_graph> frePatternICopy = new HashSet<gpm_graph>(frePatternI[i]);
			System.out.println("for pattern " + (i-1) + ": " + frePatternI[i-1].size());
			for(gpm_graph a : frePatternI[i-1]){
				if(a.graphId == 0)
					break;
				for(gpm_graph b : frePatternI[i-1]){
					boolean duplicateFlag = false;
					if(b.graphId == 0)
						break;
					if(a.graphId <= b.graphId)
						continue;
					else{
						globalCounter ++;
//						if(globalCounter % 100 == 0){
//							System.out.println("number of frequent patterns in iteration to be combined " + (i-1) + ": " + frePatternI[i-1].size() + "  Current global count: " + globalCounter);
//							}
						gpm_graph c = new gpm_graph();
						c = GraphConbination(a,b);
						if(c == null)
							continue;
						if(c.vertexSet().size() > patternSize)
								continue;
						if(frePatternI[c.vertexSet().size() - 1].size() != 0){
							for(gpm_graph g : frePatternI[c.vertexSet().size() - 1]) //check if this pattern is already in the pattern set
								if(g.edgeSetMark.containsAll(c.edgeSetMark)){
									duplicateFlag = true;
								//	System.out.println("duplicate");
									break;
								}
						}
						if(duplicateFlag == true)
							continue;
						IncJoinSimulation gsim = new IncJoinSimulation(c, dg, true, labelIndex);
						gsim.IsBSim(2, true);
						if((c.interestingness/(c.edgeSet().size() + c.vertexSet().size())) < theta)
							continue;
						else{
//							System.out.println("Size: " + divSPInit.size() + "  " +PminustopK.size() );
							CheckHashResult(c);
							frePatternI[c.vertexSet().size()-1].add(c);
							/*if(divSPInit.size() < resultSize){
								divSPInit.add(c);
								if(divSPInit.size() == resultSize){
									initdivSP(divSPInit, divSP, alpha, resultSize);
								}
							}
							else{
								if(PminustopK.size()==0){
									PminustopK.add(c);
								}else if(checkIncDiv(divSP, PminustopK, c, alpha) == false ){
									PminustopK.add(c);
								}
							}*/
							P.add(c);
						}
					}
				}
			}
			i++;
		}
		long st3 = System.currentTimeMillis();
		initdivSP(P, divSP, alpha, resultSize);
		long st4 = System.currentTimeMillis();
		System.out.println("DIversification time: " + (st4 - st3));
	}

	public void initdivSP(HashSet<gpm_graph> tempGraph, HashSet<divSP_pair> divSP_k, double alpha, int K){
		int sz = K /2;
		HashSet<gpm_graph> used= new HashSet<gpm_graph>();
		divSP_k.clear();
		
		for(int i=0;i<sz;i++){
			double mx = 0;
			gpm_graph g1= new gpm_graph();
			gpm_graph g2 = new gpm_graph();
			for(gpm_graph p1: tempGraph){
				if(!used.contains(p1)){
					for(gpm_graph p2: tempGraph){
						if((used.contains(p2)==false) && (p1.graphId > p2.graphId)){
							if( newDistance(p1,p2,alpha) > mx){
								mx = newDistance(p1, p2, alpha);
								g1 = p1;
								g2 = p2;
							}
						}
					}
				}
			}
			divSP_pair pp = new divSP_pair(g1,g2);
			used.add(g1);
			used.add(g2);
//			System.out.println("Adding a new edge in divsp init the value is "+mx+" graph ids are "+g1.graphId+" " +g2.graphId);
			divSP_k.add(pp);
		}	
	}
	
	public double newDistance(gpm_graph g1,gpm_graph g2, double alpha){
		
		double result = 0.0;
		result += alpha * g1.interestingness * (g1.vertexSet().size() + g1.edgeSet().size()); 
		result += alpha * g2.interestingness * (g2.vertexSet().size() + g2.edgeSet().size()); 
		result += 2 * (1-alpha) * Distance(g1, g2);				
		return result;
	}
	
	
	
	public void displayOnePattern (gpm_graph g,HashSet<String> finalMatchSet ){
		//System.out.println(g.graphId + "    " + g.interestingness);
		for(String s : g.simMatchSet.keySet())
			finalMatchSet.addAll(g.simMatchSet.get(s));
		System.out.println("Graph: " + g.graphId);
		for(gpm_node n : g.vertexSet()){
			System.out.println(n.tag);
		}
		for(gpm_edge e : g.edgeSet()){
			System.out.println(e.from_node + "-----" + e.to_node);
//				pw.println(e.from_node + "-----" + e.to_node);
		}
//		System.out.println("Interesting: " + g.interestingness);
	}

	
	public void displayTopKPatternFinal(HashSet<divSP_pair> patternSet, double alpha, int k){
		HashSet<gpm_graph> patternSetSingle = new HashSet<gpm_graph>();
		double disss = 0.0; 
		double finalInter = 0.0;
		int edgeNumber = 0;
		System.out.println("**********show final top k pattern**********");
//		pw.println("**********show final top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(divSP_pair g : patternSet){
			displayOnePattern(g.p1, finalMatchSet);
			displayOnePattern(g.p2, finalMatchSet);
			patternSetSingle.add(g.p1);
			patternSetSingle.add(g.p2);
			finalInter += (g.p1.interestingness * (g.p1.vertexSet().size() + g.p1.edgeSet().size()));
			finalInter += (g.p2.interestingness * (g.p2.vertexSet().size() + g.p2.edgeSet().size()));
		}
		//System.out.println(finalMatchSet);
		for(Relationship r : dg.ggdb.getAllRelationships()){
			if(finalMatchSet.contains(String.valueOf(r.getStartNode().getId())) && finalMatchSet.contains(String.valueOf(r.getEndNode().getId())))
				edgeNumber ++;
		}
		for(gpm_graph g1 : patternSetSingle)
			for(gpm_graph g2 : patternSetSingle)
				if(g1.graphId > g2.graphId){
					disss += Distance(g1,g2);
				}
		System.out.println("Final I(P)" + finalInter);
		System.out.println("Final Distance: " + disss);
		System.out.println("Final objective function: " + (alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1)));
		System.out.println("Final coverage size:" + ((finalMatchSet.size()+edgeNumber)/(double)dataGraphSize));

		System.out.println("****************************************");
	}
	
	
	//this function calculate distance of two graphs
	public double Distance(gpm_graph g1, gpm_graph g2){
		return (1.0 - minhash.similarity(g1.hashResult, g2.hashResult));
	}
	
	//this function checks if a pattern already have a hash result
	public void CheckHashResult(gpm_graph g){
		Set<Long> set = new HashSet<Long>();
		if(g.hashResult[0] != 0)
			return;
		else{
			for(String ss : g.simMatchSet.keySet())
				for(String sg :g.simMatchSet.get(ss))
					set.add(Long.valueOf(sg));
			g.hashResult = minhash.signature(set);
		}
	}
	
	
	//this function is used to combine two gpm_graph
	//if two gpm_graph share no common nodes, they can not be combined.
	@SuppressWarnings("unchecked")
	public gpm_graph GraphConbination(gpm_graph a, gpm_graph b){
		gpm_graph combinedGraph = new gpm_graph();
		combinedGraph.ConstructGraph(a);;
		boolean combineTag = false;
		
		combinedGraph.graphId = 0;
		for(gpm_node na : a.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag.equals(nb.tag)){
					combineTag = true;
					continue;
				}
			}
		
		if(combineTag == false)			//two gpm_graph can not be combined
		{	
			return null;
		}
		
		combinedGraph.gfilename = Integer.toString(graphCounter++) + "." + "sq";
		combinedGraph.graphId = graphCounter;
		for (gpm_node na : a.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag.equals(nb.tag)){
					for(String ss : b.simMatchSet.get(nb.tag))
						combinedGraph.simMatchSet.get(na.tag).add(ss);
					continue;
				}
				else{
					if(combinedGraph.GetVertex(String.valueOf(nb.tag)) == null){
						gpm_node ncopy = new gpm_node();
						ncopy.copyPN(nb);
						combinedGraph.addVertex(ncopy);
						HashSet<String> ms = new HashSet<String>();
						for(String ss : b.simMatchSet.get(nb.tag))
							ms.add(ss);
						combinedGraph.simMatchSet.put(ncopy.tag, ms);
					}
				}
			}
		
		for (gpm_edge ea : a.edgeSet())
			for(gpm_edge eb : b.edgeSet()){
				if(ea.from_node.equals(eb.from_node) && ea.to_node.equals(eb.to_node)){
					continue;
				}
				else{
					if(combinedGraph.GetEdge(eb.from_node, eb.to_node) == null){
						gpm_edge ei = new gpm_edge(eb.from_node,eb.to_node,eb.e_bound,eb.e_color);
						combinedGraph.InsertEdge(ei);
					}
				}
			}
		
		
		
		combinedGraph.edgeSetMark = new TreeSet<Integer>();
		combinedGraph.edgeSetMark.addAll(a.edgeSetMark);
		combinedGraph.edgeSetMark.addAll(b.edgeSetMark);
		combinedGraph.hashResult = new int[10000];
		for(int i = 0; i < 10000; i ++)
			combinedGraph.hashResult[i] = 0;
		if(combinedGraph.vertexSet().size() == a.vertexSet().size() || combinedGraph.vertexSet().size() == b.vertexSet().size())
			return null;
		else
			return combinedGraph;
	}
	
	public void BuildLabelIdx() throws IOException{
		if(labelIndex.invIndex.size()==0){
			labelIndex.buildLabelIndex(false);
		}
	}
	
	public void BuildRelationshipIdx() throws IOException{
		if(relIndex.relIndex.size() == 0){
			relIndex.buildRelIndex(true);
		}
	}
	
	public static void main(String[] args) throws IOException {
//		String filename = "/home/qsong/data/neo4j/YagoCores_graph_small_sample1.db";
//		String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db";
		String filename = args[0];
		System.out.println(filename);
		dg = new Neo4jGraph(filename,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{
			two_appro_basic mine = new two_appro_basic(dg);
			mine.Init();
			long st1 = System.currentTimeMillis();
			mine.PatternMining(dg, Integer.valueOf(args[1]), Double.valueOf(args[2]), Double.valueOf(args[3]), Integer.valueOf(args[4]));
//			mine.PatternMining(dg, 6, 0.0005, 0.5, 10);
			mine.displayTopKPatternFinal(mine.divSP,Double.valueOf(args[3]),Integer.valueOf(args[4]));
//			mine.displayTopKPatternFinal(mine.divSP,0.5,10);
			long st2 = System.currentTimeMillis();
			System.out.println("time: " + (st2 - st1));
			tx1.success();
		}
	}
}
