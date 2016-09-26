package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.IOException;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

public class dhopDualSimulation {
	public static gpm_graph pg;
	public static Neo4jGraph dg;
	public Label_Index labelIndex;
	public int dhop;				//number of hops
	public double quality;
	public int matchedNodes;		//number of matched nodes
	public gpm_graph[] subPatternSet;
	public HashSet<gpm_graph> finishedSet;
	public dhopDualSimulation(gpm_graph P, Neo4jGraph G, Label_Index l, int d) {
		this.pg = P;
		this.dg = G;
		labelIndex = l;
		dhop = d;
		quality = 0.0;
		matchedNodes = 0;
		subPatternSet = new gpm_graph[P.vertexSet().size()];
		finishedSet = new HashSet<gpm_graph>();
	}
	
	//induce a d-hop sub-pattern for a given node
	public gpm_graph InduceSubPattern(gpm_node coreNode){
		gpm_graph Ps = new gpm_graph();			//subpattern induced from P, d-hop node around coreode
		Ps.simMatchSet = new TreeMap<String, HashSet<String>>();
		HashSet<String> nodeMatch = new HashSet<String>();
		HashSet<gpm_node> currentNodeSet = new HashSet<gpm_node>();
		HashSet<gpm_node> nextNodeSet = new HashSet<gpm_node>();
		gpm_node ncopy = new gpm_node();
		ncopy.copyPN(coreNode);
		Ps.addVertex(ncopy);
		Ps.simMatchSet.put(ncopy.tag, nodeMatch);
		Ps.gfilename = ncopy.tag + "." + "sq";
		Ps.edgeSetMark = new TreeSet<Integer>();
		Ps.edgeSetMark.add(Integer.valueOf(ncopy.tag));
		Ps.graphId = Integer.valueOf(coreNode.tag);
		currentNodeSet.add(coreNode);
		
		boolean flag = false;
		for(int i = 1; i <= dhop; i++){
			for(gpm_node n1 : currentNodeSet){
				for(gpm_node n2 : pg.GetChildren(pg.GetVertex(n1.tag), -1)){
					for(gpm_node nn : Ps.vertexSet()){
						if(nn.tag == n2.tag){
							flag = true;
							break;
						}
					}
					if(flag == true){
						flag = false;
						continue;
					}
					ncopy = new gpm_node();
					ncopy.copyPN(n2);
					Ps.addVertex(ncopy);
					nextNodeSet.add(ncopy);
					nodeMatch = new HashSet<String>();
					nodeMatch.addAll(pg.simMatchSet.get(ncopy.tag));
					Ps.simMatchSet.put(ncopy.tag, nodeMatch);
					Ps.edgeSetMark.add(Integer.valueOf(ncopy.tag));
					gpm_edge e = new gpm_edge(n1.tag,n2.tag,1,0);
					Ps.InsertEdge(e);
				}
				for(gpm_node n2 : pg.GetParents(pg.GetVertex(n1.tag), true, -1)){
					for(gpm_node nn : Ps.vertexSet()){
						if(nn.tag == n2.tag){
							flag = true;
							break;
						}
					}
					if(flag == true){
						flag = false;
						continue;
					}
					ncopy = new gpm_node();
					ncopy.copyPN(n2);
					Ps.addVertex(ncopy);
					nextNodeSet.add(ncopy);
					nodeMatch = new HashSet<String>();
					nodeMatch.addAll(pg.simMatchSet.get(ncopy.tag));
					Ps.simMatchSet.put(ncopy.tag, nodeMatch);
					Ps.edgeSetMark.add(Integer.valueOf(ncopy.tag));
					gpm_edge e = new gpm_edge(n2.tag,n1.tag,1,0);
					Ps.InsertEdge(e);
				}
			}
			currentNodeSet.clear();
			currentNodeSet.addAll(nextNodeSet);
			nextNodeSet.clear();
		}
		return Ps;
	}

	//sort the array based on the node number of this sub-pattern(from small to large)
	void BubbleSort(gpm_graph a[], int n)
	{
	       int i, j;
	       for (i = 0; i < n; i++)
	    	   for (j = 1; j < n - i; j++)
	    		   if (a[j - 1].vertexSet().size() > a[j].vertexSet().size()){
	    			   gpm_graph temp = a[j-1];
	    			   a[j-1] = a[j];
	    			   a[j] = temp;
	    		   }
	}

	//run dual-sim for small patterns first, large patterns may re-use some result of small patterns(sub-isomorphic to it)
	//here we use node instead of edge to calculate the isomorphic (??? not sure if it will cause some problem for complex patterns)
	public double IsDDSim(int mode) throws IOException{
		HashSet<String> matchAllNode = new HashSet<String>();
		int edgeCoverCount = 0;
		int ntag = 0;
		for(gpm_node n : pg.vertexSet()){
			gpm_graph ndP = InduceSubPattern(n);
			subPatternSet[ntag++] = ndP;
		}
		BubbleSort(subPatternSet,ntag);

		for(int i = 0; i < ntag; i++){
			if(i==0)
				finishedSet.add(subPatternSet[0]);
			else{
				for(gpm_graph gg : finishedSet){
					if(subPatternSet[i].edgeSetMark.containsAll(gg.edgeSetMark)){
						for(String s : gg.simMatchSet.keySet()){						
							HashSet<String> m = subPatternSet[i].simMatchSet.get(s);
							m.addAll(gg.simMatchSet.get(s));
							subPatternSet[i].simMatchSet.remove(s);
							subPatternSet[i].simMatchSet.put(s, m);
						}
					}
				}
			}
			DualSimulation dsim = new DualSimulation(subPatternSet[i], dg, true,labelIndex);
			dsim.IsDSim(2, true);
			finishedSet.add(subPatternSet[i]);
			for(String s : subPatternSet[i].simMatchSet.keySet())
				matchAllNode.addAll(subPatternSet[i].simMatchSet.get(s));
		}
		
		for(String s1 : matchAllNode)
			for(String s2 : matchAllNode){
				if(s1 == s2)
					continue;
				Node n1 = dg.GetVertex(Long.valueOf(s1));
				Node n2 = dg.GetVertex(Long.valueOf(s2));
				if(dg.checkDistance(n1, n2))
					edgeCoverCount ++;
			}		
		pg.interestingness = (double)(matchAllNode.size() + edgeCoverCount);//return size of G_P
		System.out.println("matched nodes and edges number " + pg.interestingness + " for graph " + pg.graphId);
		return(pg.interestingness);
	}
	
	
	
	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		int mode = 2;
		String filename1 = "/Users/qsong/Downloads/Simulation_test/test_query_3.db";
		String filename2 = "/Users/qsong/Downloads/Simulation_test/test_data_3.db";

		pg = new gpm_graph();
		pg.ConstructGraphFromNeo4j(filename1, mode);
		dg = new Neo4jGraph(filename2,2);
		try(Transaction tx1 = dg.getGDB().beginTx() )
		{				
			Label_Index l = new Label_Index(dg);
			l.buildLabelIndex(false);
			dhopDualSimulation ddsim = new dhopDualSimulation(pg, dg,l,3);
			System.out.println(ddsim.IsDDSim(mode));
			tx1.success();
		}
	}
}
