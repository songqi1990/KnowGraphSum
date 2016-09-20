package GraphSum.algo;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

public class ATSum {
	public static Neo4jGraph dg;
	public Label_Index labelIndex;
	
	public String[] edgeStoreStartLabel;//store edge, each Label pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEndLabel;
	public String[] edgeStoreStart;//store edge, each related node pair is a edge, avoid duplicate edge in first iteration
	public String[] edgeStoreEnd;
	public HashSet<gpm_graph> divSP;	//current diversified summary pattern
	public HashSet<gpm_graph> P;
	public Relationship_Index relIndex;
	public HashSet<gpm_graph> frePatternI[];
	public int existingEdgeAdd;
	public Hashtable<String, Integer> nodeLabelCount;
	public Hashtable<String, Integer> edgeLabelCount;
	public Hashtable<String, String> patternLabelId; //match each label to a node id
	public int patternLabelIdCount;
	public TreeSet<Integer> pairs;							//store pattern pairs
	public Hashtable<TreeSet<Integer>,Double> pairDistance;	//store distance of pattern pairs, only store those in top-k 
	public int dataGraphSize;
	public int globalCounter; //count for debug: stores the number of combine
	public HashSet<Integer> openPatternId;		//store those open patterns in all pattern mining process
	public int couverageRange;	//range of couverage for each pattern
	public int graphCounter;	//A pattern Id for each pattern
	public File outputfile;
	public MinHash minhash;
	public Hashtable<Integer,Long> patternNodeNumberList;		//store the node match list for each edge pattern (smallest pattern), used to compute objective function value
	public Hashtable<Integer,Long> patternEdgeList;			//store the edge match list for each edge pattern (smallest pattern), used to compute objective function value
	public NeiLabelIndex neiLabelIndex;
	
	public ATSum(Neo4jGraph G) throws IOException{
		labelIndex = new Label_Index(G);
		relIndex = new Relationship_Index(G);
		//frePattern = new Hashtable<Integer,HashSet>();
		frePatternI = new HashSet[10];
		for(int i =0 ;i < 10 ;i ++){
			frePatternI[i] = new HashSet<gpm_graph>();
			//frePattern.put(i, frePatternI[i]);
		}
		openPatternId = new HashSet<Integer>();
		pairs = new TreeSet<Integer>();
		pairDistance = new Hashtable<TreeSet<Integer>,Double>();
		divSP = new HashSet<gpm_graph>();
		P = new HashSet<gpm_graph>();
		patternLabelId = new Hashtable<String,String>();
		patternLabelIdCount = 0;
		edgeStoreStart = new String[999999];
		edgeStoreEnd = new String[999999];
		edgeStoreStartLabel = new String[999999];
		edgeStoreEndLabel = new String[999999];
		nodeLabelCount = new Hashtable<String, Integer>();
		edgeLabelCount = new Hashtable<String, Integer>();
		graphCounter = 1;
		globalCounter = 0;
		existingEdgeAdd = 0;
		dataGraphSize = dg.getNodeNumber() + dg.getEdgeNumber();
		minhash = new MinHash(0.01, 10000000);
		patternEdgeList = new Hashtable<Integer,Long>();
		patternNodeNumberList = new Hashtable<Integer,Long>();
		neiLabelIndex = new NeiLabelIndex();
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
				if(CheckFrequency(edgeFre, 8000, 50000)){
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
				if(CheckFrequency(startNodeFrequency,8000, 50000) && CheckFrequency(endNodeFrequency, 8000, 50000)){
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
		neiLabelIndex.calNeighborLabelIndexIn(dg);
		neiLabelIndex.calNeighborLabelIndexOut(dg);
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

	public static void main(String[] args) throws IOException {
//		String filename = "/home/qsong/data/neo4j/YagoCores_graph_small_sample1.db";
//		String filename = args[0];		
//		String filename = "/Users/qsong/Downloads/YagoCores_graph_small_sample1.db_backup";
		String filename = "/Users/qsong/Downloads/yago.db";
		System.out.println(filename);
		dg = new Neo4jGraph(filename,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{
			ATSum mine = new ATSum(dg);
			mine.Init();

			long st1 = System.currentTimeMillis();
//			mine.PatternMining(dg, Integer.valueOf(args[1]), Double.valueOf(args[2]), Double.valueOf(args[3]),Integer.valueOf(args[4]));			
//			mine.displayTopKPatternFinal(mine.divSP, Double.valueOf(args[3]), Integer.valueOf(args[4]));
//			mine.displayTopKPatternFinal(mine.divSP,0.5,64);
//			mine.StoreTopK(mine.divSP);
//			mine.StoreQuery(mine.finalQuerySet);
			System.out.println("Frequent edges: " + mine.frePatternI[1].size());
			long st2 = System.currentTimeMillis();
			System.out.println("time: " + (st2 - st1));
			tx1.success();
		}
	}
}
