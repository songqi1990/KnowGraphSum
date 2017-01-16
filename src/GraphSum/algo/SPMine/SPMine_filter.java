//origional 2-approxinmation algorithm. The neighborhood index works in line 765 to 824 
//use closed patterns
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

import org.neo4j.graphdb.Direction;
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

public class SPMine_filter {
	public static Neo4jGraph dg;
	
	public Label_Index labelIndex;
	//public Hashtable<Integer, HashSet> frePattern;	//used to store patterns generated from each iteration
	public String[] edgeStoreStartLabel;//store edge, each Label pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEndLabel;
	public String[] edgeStoreStart;//store edge, each related node pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEnd;
	public HashSet<divSP_pair> divSP;	//current diversified summary pattern
	public HashSet<gpm_graph> divSP_single;	//current diversified summary pattern
//		public HashSet<divSP_pair> divSP1;
//		public HashSet<divSP_pair> divSP2;
//		public HashSet<divSP_pair> divSP3;
//		public HashSet<divSP_pair> divSP4;
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
	public Hashtable<String, String> patternLabelId; //match each label to a node id
	public int patternLabelIdCount;
	public int dataGraphSize;
	public int globalCounter; //count for debug: stores the number of combine
	public HashSet<Integer> openPatternId;		//store those open patterns in all pattern mining process
	public int couverageRange;	//range of couverage for each pattern
	public int graphCounter;	//A pattern Id for each pattern
	public File outputfile;
	public HashSet<String> disString; //store distuginsh words
//		public FileWriter fw;
//		public PrintWriter pw;
	public MinHash minhash;
	public HashSet<gpm_graph> finalQuerySet;
	public Hashtable<Integer,Long> patternNodeNumberList;		//store the node match list for each edge pattern (smallest pattern), used to compute objective function value
	public Hashtable<Integer,Long> patternEdgeList;			//store the edge match list for each edge pattern (smallest pattern), used to compute objective function value
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexOut; //store 1-hop label index for all nodes (for outgoing edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexOut; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternOut;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternOut;	//store 2-hop label index for this pattern node
	
	Hashtable<Long,Hashtable<String,Integer>> hop1IndexIn; //store 1-hop label index for all nodes (for incoming edge)
	Hashtable<Long,Hashtable<String,Integer>> hop2IndexIn; //store 2-hop label index for all nodes
	Hashtable<String,Hashtable<String,Integer>> hop1IndexPatternIn;	//store 1-hop label index for this pattern node
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPatternIn;	//store 2-hop label index for this pattern node
	
	
	public SPMine_filter(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		relIndex = new Relationship_Index(G);
		//frePattern = new Hashtable<Integer,HashSet>();
		frePatternI = new HashSet[10];
		for(int i =0 ;i < 10 ;i ++){
			frePatternI[i] = new HashSet<gpm_graph>();
			//frePattern.put(i, frePatternI[i]);
		}
		divSP = new HashSet<divSP_pair>();
		divSP_single = new HashSet<gpm_graph>();
		divSPInit = new HashSet<gpm_graph>();
//			
		PminustopK = new HashSet<gpm_graph>();
		P = new HashSet<gpm_graph>();
		patternLabelId = new Hashtable<String,String>();
		patternLabelIdCount = 0;
//			divSP1 = new HashSet<divSP_pair>();
//			divSP2 = new HashSet<divSP_pair>();
//			divSP3 = new HashSet<divSP_pair>();
//			divSP4 = new HashSet<divSP_pair>();
		openPatternId = new HashSet<Integer>();
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
		disString = new HashSet<String>();
		disString.add("Album");
		disString.add("Company");
		disString.add("Band");
		disString.add("Food");
		dataGraphSize = dg.getNodeNumber() + dg.getEdgeNumber();
		finalQuerySet = new HashSet<gpm_graph>();
		minhash = new MinHash(0.01, 10000000);
		patternEdgeList = new Hashtable<Integer,Long>();
		patternNodeNumberList = new Hashtable<Integer,Long>();
		hop1IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexOut = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternOut = new Hashtable<String,Hashtable<String,Integer>>();
		
		hop1IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop2IndexIn = new Hashtable<Long,Hashtable<String,Integer>>();
		hop1IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
		hop2IndexPatternIn = new Hashtable<String,Hashtable<String,Integer>>();
//			outputfile = new File("/home/qsong/program/SPMine/output.txt");
//			fw = new FileWriter("/home/qsong/program/SPMine/output.txt");
//			outputfile = new File("/Users/qsong/Downloads/output.txt");
//			fw = new FileWriter("/Users/qsong/Downloads/output.txt");
//			pw = new PrintWriter(fw);
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
				if(CheckFrequency(edgeFre, 0, 200000)){
					flag = true;
					break;
				}
			}
				
			if(flag == true){
				Node startNode = r.getStartNode();
				Node endNode = r.getEndNode();
				if(!CheckLabel(startNode) || !CheckLabel(endNode))
					continue;
				if(!CheckLabelStringSet(startNode,disString) && !CheckLabelStringSet(endNode,disString))
					continue;
				int startNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(startNode.getId()));
				int endNodeFrequency = nodeLabelCount.get(dg.maxLabelofNode.get(endNode.getId()));
				if(CheckFrequency(startNodeFrequency,0, 200000) && CheckFrequency(endNodeFrequency, 0, 200000)){
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
		
		//calculate patternNodeList
		System.out.println("start building patternNodeList");
		for(gpm_graph g : frePatternI[1]){
			long nodeN = 0;
			for(String s : g.simMatchSet.keySet())
				nodeN += g.simMatchSet.get(s).size();
			patternNodeNumberList.put(g.graphId,nodeN);
		}
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
			if(n.getId()%10000 == 0)
				System.out.println("node:" + n.getId());
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
						long edgeFre = patternEdgeList.get(g.graphId);
						edgeFre += 1;
						patternEdgeList.put(g.graphId, edgeFre);
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
	public boolean CheckLabelString(Node n, String ds){
		Iterable<Label> lset = n.getLabels();
		for(Label l : lset){
			if(l.name().contains(ds))
				return true;
		}
		return false;
	}
	
	public boolean CheckLabelStringSet(Node n, HashSet<String> ds){
		Iterable<Label> lset = n.getLabels();
		for(Label l : lset)
			for(String ss : ds){
				if(l.name().equals(ss))
					return true;
			}
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
		long edgeFre = 0;
		patternEdgeList.put(g.graphId, edgeFre);
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
			long iterationTime = 0;
			int simNumber = 0;
			if( i > 1)
				if(frePatternI[i-1].size() == 0){
					System.out.println("Pattern mining stop");
					break;
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
//							if(globalCounter % 100 == 0){
//								System.out.println("number of frequent patterns in iteration to be combined " + (i-1) + ": " + frePatternI[i-1].size() + "  Current global count: " + globalCounter);
//								}
						gpm_graph c = new gpm_graph();
						c = GraphConbination(a,b);
						if(c == null)
							continue;
						if(c.vertexSet().size() > patternSize){
							continue;
						}
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
							if(c.simMatchSet.get(s).size() > 50000)
								aaaflag=true;
						}
						if(aaaflag==true)
							continue;
						long st3 = System.currentTimeMillis();
						IncJoinSimulation gsim = new IncJoinSimulation(c, dg, true, labelIndex);
						gsim.IsBSim(2, true);
						long st4 = System.currentTimeMillis();
						iterationTime += (st4-st3);
						simNumber++;
						long Gc = 0;		//size of |G_c|
						HashSet<String> nodeL = new HashSet<String>();
						for(int s : c.edgeSetMark){
							Gc += patternEdgeList.get(s);
							Gc += patternNodeNumberList.get(s);
						}
						
						double newInterestingness = ((double)c.vertexSet().size()/patternSize)*(c.interestingness/Gc);
//						System.out.println("Interestingness: " + c.interestingness + ", Gc: " + Gc + ", final Inter: " + newInterestingness);
						c.interestingness = newInterestingness;
						if(newInterestingness < theta)
//						if(c.interestingness/c.vertexSet().size() < theta)
							continue;
						else{
							openPatternId.add(a.graphId);
							openPatternId.add(b.graphId);
							if(c.vertexSet().size() == patternSize)
								openPatternId.add(c.graphId);
//								System.out.println("Size: " + divSPInit.size() + "  " +PminustopK.size() );
							CheckHashResult(c);
							frePatternI[c.vertexSet().size()-1].add(c);
							P.add(c);
						}
					}
				}
			}
			i++;
			if(simNumber != 0)
				System.out.println("average time: " + iterationTime/simNumber);
		}
		System.out.println("all pattern size: " + P.size());
		HashSet<gpm_graph> openPattern = new HashSet<gpm_graph>();// here we delete all open patterns from the P set
		for(gpm_graph g : P){
			if(openPatternId.contains(g.graphId))
				openPattern.add(g);
//			if(g.vertexSet().size() < 6)
//				openPattern.add(g);
		}
		P.removeAll(openPattern);
		System.out.println("close pattern size: " + P.size());
		newinitdivSP(P, divSP_single, alpha, resultSize);
	}

	public void initdivSP(HashSet<gpm_graph> tempGraph, HashSet<divSP_pair> divSP_k, double alpha, int K){
		int sz = K /2;
		HashSet<gpm_graph> used= new HashSet<gpm_graph>();
		divSP_k.clear();
//		TreeSet<Integer> allEdge = new TreeSet<Integer>();
		boolean inclusionTag = false;	//false that new pattern is not included by pattern from top-k. true that one/two new pattern is included by pattern from top-k
		double mx = 0;
		while(true){
			mx = 0;
			gpm_graph g0 = new gpm_graph();
			gpm_graph g1= new gpm_graph();
			gpm_graph g2 = new gpm_graph();
//			for(gpm_graph p0:tempGraph){
//				if(allEdge.containsAll(p0.edgeSetMark))
//					used.add(p0);
//			}
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
			if(divSP_k.size() == K/2 || mx == 0)
				break;
			divSP_pair ppp = new divSP_pair(g1,g2);
			used.add(g1);
			used.add(g2);
			System.out.println(g1.graphId);
			System.out.println(g2.graphId);
			divSP_k.add(ppp);
//			allEdge.addAll(g1.edgeSetMark);
//			allEdge.addAll(g2.edgeSetMark);
			
		}	
	}
	
	public void newinitdivSP(HashSet<gpm_graph> tempGraph, HashSet<gpm_graph> divSP_k, double alpha, int K){
		HashSet<gpm_graph> used= new HashSet<gpm_graph>();
		divSP_k.clear();
//		TreeSet<Integer> allEdge = new TreeSet<Integer>();
		double mx = 0;
		while(true){
			mx = 0;
			gpm_graph g1= new gpm_graph();
			gpm_graph g2 = new gpm_graph();
//			for(gpm_graph p0:tempGraph){
//				if(allEdge.containsAll(p0.edgeSetMark))
//					used.add(p0);
//			}
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
			if(divSP_k.size() >= K || mx == 0)
				break;
			used.add(g1);
			used.add(g2);
			System.out.println(g1.graphId);
			System.out.println(g2.graphId);
//			if(g1.interestingness > g2.interestingness)
//				used.add(g2);
//			else
//				used.add(g1);
			if(divSP_k.size() < K)
				divSP_k.add(g1);
			if(divSP_k.size() < K)
				divSP_k.add(g2);
			System.out.println("size of divSP_k" + divSP_k.size());
//			allEdge.addAll(g1.edgeSetMark);
//			allEdge.addAll(g2.edgeSetMark);		
		}	
		System.out.println("size of divSP_k final" + divSP_k.size());
	}
	
	public double newDistance(gpm_graph g1,gpm_graph g2, double alpha){
		double result = 0.0;
		result += alpha * g1.interestingness * (g1.vertexSet().size() + g1.edgeSet().size()); 
		result += alpha * g2.interestingness * (g2.vertexSet().size() + g2.edgeSet().size()); 
		result += 2 * (1-alpha) * Distance(g1, g2)*PatternDistance(g1,g2);	
		return result;
	}
	
	public double PatternDistance(gpm_graph g1, gpm_graph g2){
		Set<Integer>  s1 = new HashSet<Integer>();
		Set<Integer>  s2 = new HashSet<Integer>();
		for(gpm_node ng1 : g1.vertexSet())
			s1.add(Integer.valueOf(ng1.tag));
		for(gpm_node ng2 : g2.vertexSet())
			s2.add(Integer.valueOf(ng2.tag));
		Set<Integer>  intersection = new HashSet<Integer>(s1);
        intersection.retainAll(s2);      
        Set<Integer>  union = new HashSet<Integer>(s1);
        union.addAll(s2);
        return (1.0-(double)intersection.size() / union.size());
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
//					pw.println(e.from_node + "-----" + e.to_node);
//		}
//			System.out.println("Interesting: " + g.interestingness);
	}

	
	public void displayTopKPatternFinal(HashSet<divSP_pair> patternSet, double alpha, int k){
		HashSet<gpm_graph> patternSetSingle = new HashSet<gpm_graph>();
		double disss = 0.0; 
		double finalInter = 0.0;
		int edgeNumber = 0;
		System.out.println("**********show final top k pattern**********");
//			pw.println("**********show final top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(divSP_pair g : patternSet){
			displayOnePattern(g.p1, finalMatchSet);
			displayOnePattern(g.p2, finalMatchSet);
			patternSetSingle.add(g.p1);
			patternSetSingle.add(g.p2);
			finalInter += g.p1.interestingness;
			finalInter += g.p2.interestingness;
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
		System.out.println("Final objective function: " + (alpha * finalInter + 2 * (1-alpha) * disss / (double)((patternSet.size()*2)-1)));
		System.out.println("Final coverage size:" + ((finalMatchSet.size()+edgeNumber)/(double)dataGraphSize));

		System.out.println("****************************************");
	}
	
	public void displayTopKPatternFinalSingle(HashSet<gpm_graph> patternSet, double alpha, int k){
		double disss = 0.0;
		double finalInter = 0.0;
		int edgeNumber = 0;
//		System.out.println("**********show final top k pattern**********");
		HashSet<String> finalMatchSet = new HashSet<String>();
		for(gpm_graph g : patternSet){
			finalInter += g.interestingness;
			for(String s : g.simMatchSet.keySet())
				finalMatchSet.addAll(g.simMatchSet.get(s));
			System.out.println("graph:"+g.graphId);
			for(gpm_node n : g.vertexSet()){
				System.out.println(n.tag+": " +n.nlabel);
			}
			for(gpm_edge e : g.edgeSet()){
				System.out.println(e.from_node + "-----" + e.to_node);
			}
		}
		for(Relationship r : dg.ggdb.getAllRelationships()){
			if(finalMatchSet.contains(String.valueOf(r.getStartNode().getId())) && finalMatchSet.contains(String.valueOf(r.getEndNode().getId())))
				edgeNumber ++;
		}
		for(gpm_graph g1 : patternSet)
			for(gpm_graph g2 : patternSet)
				if(g1.graphId > g2.graphId){
					disss += Distance(g1,g2);
				}
		System.out.println("Final I(P): " + finalInter);
		System.out.println("Final Distance: " + disss);
//		System.out.print("Final objective function: " + (alpha * finalInter + 2 * (1-alpha) * disss / (double)(k-1)) + "\t");
		System.out.println((alpha * finalInter + 2 * (1-alpha) * disss / (double)(patternSet.size()-1)) + "\t");
		System.out.println("Final coverage size: " + ((finalMatchSet.size()+edgeNumber)/(double)dataGraphSize));
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
		for(gpm_node n : combinedGraph.vertexSet()){
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
		System.out.println("Label index build success");
	}
	
	public void BuildRelationshipIdx() throws IOException{
		if(relIndex.relIndex.size() == 0){
			relIndex.buildRelIndex(true);
		}
		System.out.println("Relationship index build success");
	}
	
	public void StoreQuery(HashSet<gpm_graph> patternSet) throws IOException{
		String filename = "/Users/qsong/Downloads/query";
		FileWriter fw = new FileWriter(filename);
		PrintWriter pw = new PrintWriter(fw);
		pw.println(patternSet.size());
		for(gpm_graph g : patternSet){
			pw.println(g.graphId);
			pw.println(g.vertexSet().size());
			for(gpm_node n : g.vertexSet()){
				pw.println(n.tag+"	"+n.nlabel);
			}
			pw.println(g.edgeSet().size());
			for(gpm_edge e : g.edgeSet()){
				pw.println(e.from_node+"	"+e.to_node+"	"+e.e_bound+"	"+e.e_color);
			}
		}
		pw.close();
		fw.close();
	}
	
	public void StoreTopK(HashSet<gpm_graph> patternSet) throws IOException{
		String filename = "/home/qsong/program/SPMine/yagoPatterns";
		FileWriter fw = new FileWriter(filename);
		PrintWriter pw = new PrintWriter(fw);
		pw.println(patternSet.size());
		for(gpm_graph g : patternSet){
			pw.println(g.graphId);
			pw.println(g.vertexSet().size());
			for(gpm_node n : g.vertexSet()){
				pw.println(n.tag+"	"+n.nlabel);
			}
			pw.println(g.edgeSet().size());
			for(gpm_edge e : g.edgeSet()){
				pw.println(e.from_node+"	"+e.to_node+"	"+e.e_bound+"	"+e.e_color);
			}
			for(String s : g.simMatchSet.keySet()){
				pw.println(s);
				pw.println(g.simMatchSet.get(s).size());
				for(String ss : g.simMatchSet.get(s))
					pw.println(ss);
			}
		}
		pw.close();
		fw.close();
	}
	
	public static void main(String[] args) throws IOException {
//			String filename = "/Users/qsong/Downloads/yago.db";
//			String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup";
		String filename = args[0];
		System.out.println(filename);
		dg = new Neo4jGraph(filename,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{
			SPMine_filter mine = new SPMine_filter(dg);
			mine.Init();
			long st1 = System.currentTimeMillis();
			mine.PatternMining(dg, Integer.valueOf(args[1]), Double.valueOf(args[2]), Double.valueOf(args[3]), Integer.valueOf(args[4]));
//				mine.PatternMining(dg, 6, 0.0005, 0.5, 10);
			mine.displayTopKPatternFinalSingle(mine.divSP_single,Double.valueOf(args[3]),Integer.valueOf(args[4]));
//			mine.StoreQuery(mine.finalQuerySet);
//			mine.StoreTopK(mine.divSP_single);
//				mine.displayTopKPatternFinalSingle(mine.divSP_single,0.5,10);
			long st2 = System.currentTimeMillis();
			System.out.println("time: " + (st2 - st1));
			tx1.success();
		}
	}
}

