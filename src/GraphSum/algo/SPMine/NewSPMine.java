//anytime algorithm based on 2-approximation
package wsu.eecs.mlkd.KGQuery.algo.SPMine;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.AVpair;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Label_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Relationship_Index;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.dhopDualSimulation;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.IncJoinSimulation;
import wsu.eecs.mlkd.KGQuery.algo.SPMine.MinHash;

public class NewSPMine {
	public static Neo4jGraph dg;
	
	public Label_Index labelIndex;
	//public Hashtable<Integer, HashSet> frePattern;	//used to store patterns generated from each iteration
	public String[] edgeStoreStartLabel;//store edge, each Label pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEndLabel;
	public String[] edgeStoreStart;//store edge, each related node pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEnd;
	public Hashtable<String, String> patternLabelId; //match each label to a node id
	public int patternLabelIdCount;
	public HashSet<newdivSP_pair> divSP;	//current diversified summary pattern
//	public HashSet<newdivSP_pair> divSP1;
//	public HashSet<newdivSP_pair> divSP2;
//	public HashSet<newdivSP_pair> divSP3;
//	public HashSet<newdivSP_pair> divSP4;
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
	
	public Hashtable<gpm_graph, PriorityQueue<newdivSP_pair> > PatternEdges;
	
	public int dataGraphSize;
	public int globalCounter; //count for debug: stores the number of combine
	public int totalPatternCounter; 	
	public int couverageRange;	//range of couverage for each pattern
	public int graphCounter;	//A pattern Id for each pattern
	public File outputfile;
//	public FileWriter fw;
//	public PrintWriter pw;
	public MinHash minhash;
	
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexOut; //store 1-hop label index for all nodes (for outgoing edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexOut; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternOut;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternOut;	//store 2-hop label index for this pattern node
	
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexIn; //store 1-hop label index for all nodes (for incoming edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexIn; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternIn;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternIn;	//store 2-hop label index for this pattern node
	

	static class PQsortrev implements Comparator<newdivSP_pair> {
		 
		public int compare(newdivSP_pair one, newdivSP_pair two) {
			double diff = two.pairWeight - one.pairWeight;
			if(diff<0) return -1;
			else if (diff>0) return 1;
			else return 0;
		}
	}
 
	static class PQsort implements Comparator<newdivSP_pair> {
		 
		public int compare(newdivSP_pair one, newdivSP_pair two) {
			double diff = two.pairWeight - one.pairWeight;
			if(diff>0) return -1;
			else if (diff<0) return 1;
			else return 0;
		}
	}
	public NewSPMine(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		relIndex = new Relationship_Index(G);
		//frePattern = new Hashtable<Integer,HashSet>();
		frePatternI = new HashSet[10];
		for(int i =0 ;i < 10 ;i ++){
			frePatternI[i] = new HashSet<gpm_graph>();
			//frePattern.put(i, frePatternI[i]);
		}
		divSP = new HashSet<newdivSP_pair>();
		divSPInit = new HashSet<gpm_graph>();
//		
		PminustopK = new HashSet<gpm_graph>();
		P = new HashSet<gpm_graph>();
		patternLabelId = new Hashtable<String,String>();
		patternLabelIdCount = 0;
//		divSP1 = new HashSet<newdivSP_pair>();
//		divSP2 = new HashSet<newdivSP_pair>();
//		divSP3 = new HashSet<newdivSP_pair>();
//		divSP4 = new HashSet<newdivSP_pair>();
		
		edgeStoreStart = new String[999999];
		edgeStoreEnd = new String[999999];
		edgeStoreStartLabel = new String[999999];
		edgeStoreEndLabel = new String[999999];
		nodeLabelCount = new Hashtable<String, Integer>();
		edgeLabelCount = new Hashtable<String, Integer>();
		PatternEdges = new Hashtable<gpm_graph, PriorityQueue<newdivSP_pair> >();
		graphCounter = 1;
		globalCounter = 0;
		existingEdgeAdd = 0;
		isExistPat = false;
		
		dataGraphSize = dg.getNodeNumber() + dg.getEdgeNumber();
	
		minhash = new MinHash(0.01, 10000000);
		hop1IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		
		hop1IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
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
				
			CalculateMaxLabel(r.getStartNode());
			CalculateMaxLabel(r.getEndNode());
				
			for(String k : r.getPropertyKeys()){
				edgeFre = (int)edgeLabelCount.get(r.getProperty(k).toString());
				if(CheckFrequency(edgeFre, 5000, 90000)){
					flag = true;
					break;
				}
			}
				
			if(flag == true){
				Node startNode = r.getStartNode();
				Node endNode = r.getEndNode();
				if(!CheckLabel(startNode) || !CheckLabel(endNode))
					continue;
				int startNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(startNode.getId()));
				int endNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(endNode.getId()));
				if(CheckFrequency(startNodeFrequency,5000, 90000) && CheckFrequency(endNodeFrequency, 5000, 90000)){
					if(CheckEdgeDuplicate(r, dg.maxLabelofNode.get(startNode.getId()),dg.maxLabelofNode.get(endNode.getId()))){
						newGraph = AddFreEdgetoGraph(r);
//						System.out.println(newGraph.graphId);
//						for(gpm_node n : newGraph.vertexSet())
//							System.out.println(n.tag + ": " + n.nlabel);
						frePatternI[1].add(newGraph);
						edgeStoreStartLabel[graphCounter] = dg.maxLabelofNode.get(startNode.getId());
						edgeStoreEndLabel[graphCounter] = dg.maxLabelofNode.get(endNode.getId());
						edgeStoreStart[graphCounter] = patternLabelId.get(dg.maxLabelofNode.get(startNode.getId()));
						edgeStoreEnd[graphCounter] = patternLabelId.get(dg.maxLabelofNode.get(endNode.getId()));
						graphCounter ++;
					}
				}
			}
		}
		calNeighborLabelIndexOut();
		calNeighborLabelIndexIn();
		System.out.println("number of frequent patterns in iteration 1: " + frePatternI[1].size());
		return true;
	}

	public void calNeighborLabelIndexOut(){
		System.out.println("Start building 1-hop and 2-hop neighbor lable index (outgoing)");
		HashSet<Node> hop1Node = new HashSet<Node>();
		HashSet<Node> hop2Node = new HashSet<Node>();
		for(Node n : dg.ggdb.getAllNodes()){
			hop1Node.clear();
			hop2Node.clear();
			if(!hop1IndexOut.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexOut.put(n.getId(), ll);
			}
			if(!hop2IndexOut.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexOut.put(n.getId(), ll);
			}
			for(Relationship r : n.getRelationships(Direction.OUTGOING)){
				hop1Node.add(r.getEndNode());
				String neiLabel = dg.maxLabelofNode.get(r.getEndNode().getId());
				if(!hop1IndexOut.get(n.getId()).containsKey(neiLabel)){
					hop1IndexOut.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop1IndexOut.get(n.getId()).get(neiLabel)+1;
					hop1IndexOut.get(n.getId()).remove(neiLabel);
					hop1IndexOut.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
			for(Node hop1 : hop1Node){
				for(Relationship r : hop1.getRelationships(Direction.OUTGOING)){
					hop2Node.add(r.getEndNode());
				}
			}
			for(Node hop2 : hop2Node){
				String neiLabel = dg.maxLabelofNode.get(hop2.getId());
				if(!hop2IndexOut.get(n.getId()).containsKey(neiLabel)){
					hop2IndexOut.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop2IndexOut.get(n.getId()).get(neiLabel)+1;
					hop2IndexOut.get(n.getId()).remove(neiLabel);
					hop2IndexOut.get(n.getId()).put(neiLabel, newFrequency);
				}		
			}
		}
		System.out.println("1-hop and 2-hop neighbor lable index build success (outgoing)");
	}
	
	public void calNeighborLabelIndexIn(){
		System.out.println("Start building 1-hop and 2-hop neighbor lable index (incoming)");
		HashSet<Node> hop1Node = new HashSet<Node>();
		HashSet<Node> hop2Node = new HashSet<Node>();
		for(Node n : dg.ggdb.getAllNodes()){
			hop1Node.clear();
			hop2Node.clear();
			if(!hop1IndexIn.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexIn.put(n.getId(), ll);
			}
			if(!hop2IndexIn.containsKey(n.getId())){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexIn.put(n.getId(), ll);
			}
			for(Relationship r : n.getRelationships(Direction.INCOMING)){
				hop1Node.add(r.getStartNode());
				String neiLabel = dg.maxLabelofNode.get(r.getStartNode().getId());
				if(!hop1IndexIn.get(n.getId()).containsKey(neiLabel)){
					hop1IndexIn.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop1IndexIn.get(n.getId()).get(neiLabel)+1;
					hop1IndexIn.get(n.getId()).remove(neiLabel);
					hop1IndexIn.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
			for(Node hop1 : hop1Node){
				for(Relationship r : hop1.getRelationships(Direction.INCOMING)){
					hop2Node.add(r.getStartNode());
				}
			}
			for(Node hop2 : hop2Node){
				String neiLabel = dg.maxLabelofNode.get(hop2.getId());
				if(!hop2IndexIn.get(n.getId()).containsKey(neiLabel)){
					hop2IndexIn.get(n.getId()).put(neiLabel, 1);
				}
				else{
					int newFrequency = hop2IndexIn.get(n.getId()).get(neiLabel)+1;
					hop2IndexIn.get(n.getId()).remove(neiLabel);
					hop2IndexIn.get(n.getId()).put(neiLabel, newFrequency);
				}
			}
		}
		System.out.println("1-hop and 2-hop neighbor lable index build success (incoming)");
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
						g.simMatchSet.get(edgeStoreStart[i]).add(String.valueOf(re.getStartNode().getId()));
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
		if(patternLabelId.keySet().contains(dg.maxLabelofNode.get(r.getStartNode().getId())))
			ntag1 = patternLabelId.get(dg.maxLabelofNode.get(r.getStartNode().getId()));
		else{
			ntag1 = String.valueOf(patternLabelIdCount++);
			patternLabelId.put(dg.maxLabelofNode.get(r.getStartNode().getId()), ntag1);
		}
		nweight = 0.0;
		nlabel1 = dg.maxLabelofNode.get(r.getStartNode().getId());
		gpm_node startNode = new gpm_node(alist,ntag1,nweight,nlabel1);	
		g.addVertex(startNode);
		nodeMatch1.add(Long.toString(r.getStartNode().getId()));
		
		alist.clear();
		if(patternLabelId.keySet().contains(dg.maxLabelofNode.get(r.getEndNode().getId())))
			ntag2 = patternLabelId.get(dg.maxLabelofNode.get(r.getEndNode().getId()));
		else{
			ntag2 = String.valueOf(patternLabelIdCount++);
			patternLabelId.put(dg.maxLabelofNode.get(r.getEndNode().getId()), ntag2);
		}
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
		long st = System.currentTimeMillis();
		int outTag = 0;
		totalPatternCounter = 0;
		
		for(int i = 2 ; i < patternSize; ){
			if( i > 1)
				if(frePatternI[i-1].size() == 0){
					System.out.println("Pattern mining stop");
					return;
				 }
//			globalCounter = 0;
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
						boolean aaaflag = false;
						for(String s : c.simMatchSet.keySet()){
//							if(i>=3)
//								System.out.println(s + ": " + c.simMatchSet.get(s).size() + "\t");
							if(c.simMatchSet.get(s).size() > 2000)
								aaaflag=true;
						}
						if(aaaflag==true)
							continue;
//						IncJoinSimulation gsim = new IncJoinSimulation(c, dg, true, labelIndex);
//						gsim.IsBSim(2, true);
						dhopDualSimulation ddsim = new dhopDualSimulation(c,dg,labelIndex,1);
						ddsim.IsDDSim(2);
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
							makePairsAndInsert(c, alpha, resultSize-60);
							P.add(c);
							outTag++;
							calculateTopK(resultSize);
							//if(outTag % 5 == 0){
								//displayTopKPatternFinal(divSP,0.5,divSP.size()*2);
							long stt = System.currentTimeMillis();
							//	System.out.println(stt-st);
							//}
							totalPatternCounter++;
							System.out.println(totalPatternCounter+"\t"+(stt-st)+ "\t"+ getObjFuncValue(divSP,0.5,divSP.size()*2));
						}
					}
				}
			}
			i++;
		}
		System.out.println("Final set size: " + divSP.size());
		//displayTopKPatternFinal(divSP,0.5,64);
		divSP.clear();
//		initdivSP(P, divSP, alpha, resultSize);
		
	}
	public void makePairsAndInsert(gpm_graph c, double alpha ,int K){
		PQsort comp = new PQsort ();
		PriorityQueue<newdivSP_pair> newPQ = new PriorityQueue<newdivSP_pair> (K,comp);
		PatternEdges.put(c, newPQ);
		
		
		for(gpm_graph p: P){
			newdivSP_pair pp = new newdivSP_pair(p,c);
			pp.pairWeight = newDistance(p, c, alpha);
			
			removeMinElem(PatternEdges.get(c), pp, K);
			
			removeMinElem(PatternEdges.get(p), pp, K);
			
		}

	}
	public void removeMinElem(PriorityQueue<newdivSP_pair> tempPQ1, newdivSP_pair pp, int K){
		if(tempPQ1.size()<K){
			tempPQ1.add(pp);
		}else{
			if(tempPQ1.peek().pairWeight < pp.pairWeight){
				tempPQ1.poll();
				tempPQ1.add(pp);
			}
		}
	}
	
	public void calculateTopK(int K){
		Enumeration<gpm_graph> enumKey = PatternEdges.keys();
		PQsortrev comp = new PQsortrev ();
		PriorityQueue<newdivSP_pair> newPQ = new PriorityQueue<newdivSP_pair> (comp);
		
		while(enumKey.hasMoreElements()) {
			gpm_graph g = enumKey.nextElement();
			PriorityQueue<newdivSP_pair>  pq = PatternEdges.get(g);
			
		    for(newdivSP_pair pp: pq){
		    	newPQ.add(pp);
		    }
		}
		HashSet<gpm_graph> used= new HashSet<gpm_graph>();
		divSP.clear();
		int k=0;
		while(newPQ.size()>0){
			if(k>=K) break;
			newdivSP_pair pp = newPQ.poll();
			if(used.contains(pp.p1)){
				continue;
			}
			if(used.contains(pp.p2)){
				continue;
			}
			k+=2;
			used.add(pp.p1);
			used.add(pp.p2);
			divSP.add(pp);
		}
	}

	public void initdivSP(HashSet<gpm_graph> tempGraph, HashSet<newdivSP_pair> divSP_k, double alpha, int K){
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
			newdivSP_pair pp = new newdivSP_pair(g1,g2);
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
//		System.out.println("Graph: " + g.graphId);
//		for(gpm_node n : g.vertexSet()){
//			System.out.println(n.tag);
//		}
//		for(gpm_edge e : g.edgeSet()){
//			System.out.println(e.from_node + "-----" + e.to_node);
//				pw.println(e.from_node + "-----" + e.to_node);
//		}
//		System.out.println("Interesting: " + g.interestingness);
	}

	
	public void displayTopKPatternFinal(HashSet<newdivSP_pair> patternSet, double alpha, int k){
		HashSet<gpm_graph> patternSetSingle = new HashSet<gpm_graph>();
		double disss = 0.0; 
		double finalInter = 0.0;
		int edgeNumber = 0;
//		System.out.println("**********show final top k pattern**********");
//		pw.println("**********show final top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(newdivSP_pair g : patternSet){
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
//		System.out.println("Final I(P)" + finalInter);
//		System.out.println("Final Distance: " + disss);
//		System.out.print("Final objective function: " + (alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1)));
		System.out.print(alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1) + "\t");
//		System.out.println("Final coverage size:" + ((finalMatchSet.size()+edgeNumber)/(double)dataGraphSize));

//		System.out.println("****************************************");
	}
	public double getObjFuncValue(HashSet<newdivSP_pair> patternSet, double alpha, int k){
		HashSet<gpm_graph> patternSetSingle = new HashSet<gpm_graph>();
		double disss = 0.0; 
		double finalInter = 0.0;
		int edgeNumber = 0;
//		System.out.println("**********show final top k pattern**********");
//		pw.println("**********show final top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(newdivSP_pair g : patternSet){
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
//		System.out.println("Final I(P)" + finalInter);
//		System.out.println("Final Distance: " + disss);
//		System.out.print("Final objective function: " + (alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1)));
		return (alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1) );
//		System.out.println("Final coverage size:" + ((finalMatchSet.size()+edgeNumber)/(double)dataGraphSize));

//		System.out.println("****************************************");
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
		hop1IndexPatternOut.clear();
		hop2IndexPatternOut.clear();
		hop1IndexPatternIn.clear();
		hop2IndexPatternIn.clear();
		gpm_graph combinedGraph = new gpm_graph();
		combinedGraph.ConstructGraph(a);;
		boolean combineTag = false;
		
		combinedGraph.graphId = 0;
		for(gpm_node na : a.vertexSet())
			for(gpm_node nb : b.vertexSet()){
				if(na.tag.equals(nb.tag)){
					combineTag = true;
					break;
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
						if(!combinedGraph.simMatchSet.get(na.tag).contains(ss))
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
		
		//calculate 1-hop and 2-hop neighbor label index for this pattern.
		calNeighborLabelIndexPatternOut(combinedGraph);
		calNeighborLabelIndexPatternIn(combinedGraph);
		//refine the matchset based on the neighbor label index
		for(gpm_node n : combinedGraph.vertexSet()){
			HashSet<String> nodeFiltered = new HashSet<String>();
			for(String matchNode : combinedGraph.simMatchSet.get(n.tag)){
				boolean filterFlag = false;
				for(String matchLabel : hop1IndexPatternOut.get(n.tag).keySet()){
					if(!hop1IndexOut.get(Long.valueOf(matchNode)).keySet().contains(matchLabel)){
						filterFlag = true;
						break;
					}
					if(hop1IndexOut.get(Long.valueOf(matchNode)).get(matchLabel) < hop1IndexPatternOut.get(n.tag).get(matchLabel)){
						filterFlag = true;
						break;
					}
				}
				for(String matchLabel : hop2IndexPatternOut.get(n.tag).keySet()){
					if(!hop2IndexOut.get(Long.valueOf(matchNode)).keySet().contains(matchLabel)){
						filterFlag = true;
						break;
					}
					if(hop2IndexOut.get(Long.valueOf(matchNode)).get(matchLabel) < hop2IndexPatternOut.get(n.tag).get(matchLabel)){
						filterFlag = true;
						break;
					}
				}
				
				for(String matchLabel : hop1IndexPatternIn.get(n.tag).keySet()){
					if(!hop1IndexIn.get(Long.valueOf(matchNode)).keySet().contains(matchLabel)){
						filterFlag = true;
						break;
					}
					if(hop1IndexIn.get(Long.valueOf(matchNode)).get(matchLabel) < hop1IndexPatternIn.get(n.tag).get(matchLabel)){
						filterFlag = true;
						break;
					}
				}
				for(String matchLabel : hop2IndexPatternIn.get(n.tag).keySet()){
					if(!hop2IndexIn.get(Long.valueOf(matchNode)).keySet().contains(matchLabel)){
						filterFlag = true;
						break;
					}
					if(hop2IndexIn.get(Long.valueOf(matchNode)).get(matchLabel) < hop2IndexPatternIn.get(n.tag).get(matchLabel)){
						filterFlag = true;
						break;
					}
				}
				
				if(filterFlag == true){
					nodeFiltered.add(matchNode);
				}
			}
			for(String s : nodeFiltered){
				combinedGraph.simMatchSet.get(n.tag).remove(s);
			}
			if(combinedGraph.simMatchSet.get(n.tag).size() == 0)
				return null;
		}
		if(combinedGraph.vertexSet().size() == a.vertexSet().size() || combinedGraph.vertexSet().size() == b.vertexSet().size())
			return null;
		else
			return combinedGraph;
	}
	
	public void calNeighborLabelIndexPatternOut(gpm_graph gtest){
		HashSet<gpm_node> hop1NodePattern = new HashSet<gpm_node>();
		HashSet<gpm_node> hop2NodePattern = new HashSet<gpm_node>();
		for(gpm_node n : gtest.vertexSet()){
			hop1NodePattern.clear();
			hop2NodePattern.clear();
			if(!hop1IndexPatternOut.containsKey(n.tag)){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexPatternOut.put(n.tag, ll);
			}
			if(!hop2IndexPatternOut.containsKey(n.tag)){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexPatternOut.put(n.tag, ll);
			}
			for(gpm_edge r : gtest.outgoingEdgesOf(n)){
				gpm_node n1 = gtest.GetVertex(r.to_node);
				hop1NodePattern.add(n1);
				if(!hop1IndexPatternOut.get(n.tag).containsKey(n1.nlabel)){
					hop1IndexPatternOut.get(n.tag).put(n1.nlabel, 1);
				}
				else{
					int newFrequency = hop1IndexPatternOut.get(n.tag).get(n1.nlabel)+1;
					hop1IndexPatternOut.get(n.tag).remove(n1.nlabel);
					hop1IndexPatternOut.get(n.tag).put(n1.nlabel, newFrequency);
				}
			}
			for(gpm_node hop1 : hop1NodePattern){
				for(gpm_edge r : gtest.outgoingEdgesOf(hop1)){
					hop2NodePattern.add(gtest.GetVertex(r.to_node));
				}
			}
			for(gpm_node n2 : hop2NodePattern){
				if(!hop2IndexPatternOut.get(n.tag).containsKey(n2.nlabel)){
					hop2IndexPatternOut.get(n.tag).put(n2.nlabel, 1);
				}
				else{	
					int newFrequency = hop2IndexPatternOut.get(n.tag).get(n2.nlabel)+1;
					hop2IndexPatternOut.get(n.tag).remove(n2.nlabel);
					hop2IndexPatternOut.get(n.tag).put(n2.nlabel, newFrequency);
				}
			}
		}
	}
	
	public void calNeighborLabelIndexPatternIn(gpm_graph gtest){
		HashSet<gpm_node> hop1NodePattern = new HashSet<gpm_node>();
		HashSet<gpm_node> hop2NodePattern = new HashSet<gpm_node>();
		for(gpm_node n : gtest.vertexSet()){
			hop1NodePattern.clear();
			hop2NodePattern.clear();
			if(!hop1IndexPatternIn.containsKey(n.tag)){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop1IndexPatternIn.put(n.tag, ll);
			}
			if(!hop2IndexPatternIn.containsKey(n.tag)){
				Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
				hop2IndexPatternIn.put(n.tag, ll);
			}
			for(gpm_edge r : gtest.incomingEdgesOf(n)){
				gpm_node n1 = gtest.GetVertex(r.from_node);
				hop1NodePattern.add(n1);
				if(!hop1IndexPatternIn.get(n.tag).containsKey(n1.nlabel)){
					hop1IndexPatternIn.get(n.tag).put(n1.nlabel, 1);
				}
				else{
					int newFrequency = hop1IndexPatternIn.get(n.tag).get(n1.nlabel)+1;
					hop1IndexPatternIn.get(n.tag).remove(n1.nlabel);
					hop1IndexPatternIn.get(n.tag).put(n1.nlabel, newFrequency);
				}
			}
			for(gpm_node hop1 : hop1NodePattern){
				for(gpm_edge r : gtest.incomingEdgesOf(hop1)){
					hop2NodePattern.add(gtest.GetVertex(r.from_node));
				}
			}
			for(gpm_node n2 : hop2NodePattern){
				if(!hop2IndexPatternIn.get(n.tag).containsKey(n2.nlabel)){
					hop2IndexPatternIn.get(n.tag).put(n2.nlabel, 1);
				}
				else{
					int newFrequency = hop2IndexPatternIn.get(n.tag).get(n2.nlabel)+1;
					hop2IndexPatternIn.get(n.tag).remove(n2.nlabel);
					hop2IndexPatternIn.get(n.tag).put(n2.nlabel, newFrequency);
				}
			}
		}
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
//		String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup";
		String filename = args[0];
		System.out.println(filename);
		dg = new Neo4jGraph(filename,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{
			NewSPMine mine = new NewSPMine(dg);
			mine.Init();
			long st1 = System.currentTimeMillis();
			mine.PatternMining(dg, Integer.valueOf(args[1]), Double.valueOf(args[2]), Double.valueOf(args[3]), Integer.valueOf(args[4]));
//			mine.PatternMining(dg, 6, 0.0008, 0.5, 64);
//			mine.displayTopKPatternFinal(mine.divSP,Double.valueOf(args[3]),Integer.valueOf(args[4]));
//			mine.displayTopKPatternFinal(mine.divSP,0.5,64);
			long st2 = System.currentTimeMillis();
			System.out.println("time: " + (st2 - st1));
			tx1.success();
		}
	}
}
