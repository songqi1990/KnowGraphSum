package wsu.eecs.mlkd.KGQuery.algo.joinsim;

/*
 * This is the incremental version of joinsim used for SPMine algo
 * Take a query graph, a data graph, an label index as input.
 * Query graph contains a previous match set for each node D
 * Do not need to generate cansets for those nodes who already have a matching set.
 * Generate cansets for those new nodes(empty matching set) from labelIndex
 * Write output directly to the query graph 
 */

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Vector;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;

import org.neo4j.graphdb.*;

//This class computes the bounded simulation 
//between pattern and data graphs.

public class IncJoinSimulation {
	public static gpm_graph pg;
	public static Neo4jGraph dg;

	public TreeMap<String, Vector<String>> matchSet;
	public TreeMap<String, Vector<String>> canSet;
	public Property_Index propertyIndex;
	public Label_Index labelIndex;
	public HashSet<String> matchingset;
	public HashSet<String> candidateset;
	public HashSet<String> reducedmatchingset;
	public int dgGraphSize;
	public int idxmode;
	public int realAFF1 = 0;// those really possible affects matches.

	public long distquerybfs = 0;
	public long distquerybibfs = 0;

	// this opt is for distance buffer checking.
	// if set to true,
	// the checking of distance buffer is under optimization mode.
	public boolean checkopt = true;

	public IncJoinSimulation(gpm_graph P, Neo4jGraph G, boolean opt, Label_Index l) {

		this.pg = P;
		this.dg = G;
		matchSet = new TreeMap<String, Vector<String>>();
		matchingset = new HashSet<String>();
		reducedmatchingset = new HashSet<String>();
		candidateset = new HashSet<String>();
		labelIndex = l;
		canSet = new TreeMap<String, Vector<String>>();
		checkopt = opt;
		dgGraphSize = G.nodes.size() + G.edges.size();
	}



	// ============================================================================//
	// display result
	// ============================================================================//
	public void displayMatch() {

		System.out.println("Displaying Match:");
		
		for (String pn : matchSet.keySet()) {
			System.out.print(pn + ":");
			for (String mn : matchSet.get(pn)) {
				System.out.print(mn + " ");
			}
			System.out.println();
		}
	}

	// ============================================================================//
	// initialize sim candidate set
	// ============================================================================//
	public boolean Init() throws IOException {	
		for (gpm_node pn : pg.vertexSet()) {
			HashSet<String> ans = new HashSet<String>();			
			if(pg.simMatchSet.containsKey(pn.tag))
				if(pg.simMatchSet.get(pn.tag).size() != 0)
					for(String s : pg.simMatchSet.get(pn.tag))
						ans.add(s);
				else{
					//System.out.println("need to access label index");
					return false;
				}
			else{
				ans = labelIndex.getLabelAnswer(pn.nlabel);
				System.out.println("need to access label index" + pn.tag);
			}
			if (ans == null || ans.size() == 0) {
				System.out.println("empty sim set.");
				return false;
			}
			Vector<String> ansvec = new Vector<String>();
			Vector<String> canvec = new Vector<String>();
			ansvec.addAll(ans);
			matchSet.put(pn.tag, ansvec);
			canSet.put(pn.tag, canvec);
		}
		pg.simMatchSet.clear();
		return true;
	}

	// ============================================================================//
	// This function performs the sim join for two pattern nodes
	// idx mode: 1: matrix; 2: distance buffer
	// If change: return 1;
	// If no change: return 0;
	// If reduce to zero: return -1; (not bsim)
	// ============================================================================//

	public int simJoin(gpm_node a, gpm_node b) {
		Vector<String> mseta = matchSet.get(a.tag);
		Vector<String> msetb = matchSet.get(b.tag);
		Vector<String> canseta = canSet.get(a.tag);
		boolean change = false;
		boolean remv = true;
		
		for (int i = 0; i < mseta.size(); i++) {
			remv = true;
			for (String pbsim : msetb) {
				if(dg.checkDistance(dg.GetVertex(Long.valueOf(mseta.elementAt(i))), dg.GetVertex(Long.valueOf(pbsim)))){
					remv = false;
					break;
				}
			}
			if (remv) {
				canseta.add(mseta.elementAt(i));
				mseta.remove(i);
				i--;
				change = true;
				if (mseta.size() == 0)
					return -1;
			}
		}
		matchSet.put(a.tag, mseta);
		canSet.put(a.tag, canseta);
		if (change)
			return 1;
		return 0;
	}

	// ============================================================================//
	// return matching size
	// ============================================================================//
	public void msize() {
		matchingset.clear();
		candidateset.clear();
		int minsize = Integer.MAX_VALUE;
		int maxsize = -1;
		int size = 0;
		for (String s : matchSet.keySet()) {
			Vector<String> mset = matchSet.get(s);
			size = mset.size();
			for (String m : mset) {
				matchingset.add(s + ":" + m);
			}
			if (size < minsize)
				minsize = size;
			if (size > maxsize)
				maxsize = size;
		}

		for (String s : canSet.keySet()) {
			Vector<String> cset = matchSet.get(s);

			for (String c : cset) {
				candidateset.add(s + ":" + c);
			}
		}
	}

	// =====================================================================//
	// This function defines comparator for Nodes
	// ====================================================================//
	public class simsetComparator implements Comparator<gpm_node>{
		@Override
		public int compare(gpm_node a, gpm_node b){
			if (matchSet.get(a.tag).size() < matchSet.get(b.tag).size()){
				return -1;
			}
			if (matchSet.get(a.tag).size() > matchSet.get(b.tag).size()){
				return 1;
			}
			return 0;
		}
	}

	// =====================================================================//
	// main process of bounded sim checking -- with join over sim sets
	// =====================================================================//
	@SuppressWarnings({})
	public double IsBSim(int idxmode, boolean opt) throws IOException {
		this.idxmode = idxmode;

		// step 1: init. return false if for some node, init sim set is empty.
		if (!Init()) {
			return 0.0;
		}
		
		// step 2: topsorting and ranking
		gpm_topSort gsort = new gpm_topSort(pg);
		Vector<gpm_node> sccnodes = gsort.sortedsccnodes;
		if(!opt){
			Collections.shuffle(sccnodes);
		}

		// step 3: sim join with topological order
		// the nodes with larger rank (low topological order) is processed
		// first.
		Vector<gpm_node> nset = new Vector<gpm_node>();
		int result = 0;

		if (opt) {
			for ( gpm_node abpn : sccnodes) {
				nset.clear();
				nset.addAll(pg.expSCC(abpn));
				Comparator<gpm_node> comparator = new simsetComparator();
				Collections.sort(nset, comparator);
				boolean change = true;
				// scc inner join
				while (change) {
					change = false;
					for (gpm_node a : nset) {
						Vector<gpm_node> pvec = pg.GetParents(a, true, -1);
						for (gpm_node pa : pg.GetParents(a, true, -1)) {
							result = simJoin(pa, a);
							if (result == 1)
								change = true;
							else if (result == 0)
								change = false;
							else if (result == -1) {
								return 0.0;
							}
						}
					}
				}
			}
		}

		// step 4: collect result
		//msize();
//		System.out.println("After sim");
//		for(String key : matchSet.keySet())
//			System.out.println("Size for " + key + ": " + matchSet.get(key).size());
//		
		for (String pn : matchSet.keySet()) {
			HashSet<String> v = new HashSet<String>();
			for(String ss : matchSet.get(pn))
				v.add(ss);
			pg.simMatchSet.put(pn, v);
		}
		
		pg.interestingness = interestingnessCal();	// move the new interestingness calculation to SPMine, thus here returns the size of |G_p|

		return pg.interestingness;
	}

	public double interestingnessCal(){
		Vector<String> matchAllNode = new Vector<String>();
		//for(String s : matchSet.keySet())
			//System.out.println("Match set for " + s + matchSet.get(s));
		int edgeCoverCount = 0;
		for (String pn : matchSet.keySet()){
			for (String mn : matchSet.get(pn)) {
				if(!matchAllNode.contains(mn))
					matchAllNode.addElement(mn);
			}
		}
		
		for(gpm_edge e : pg.edgeSet()){
			for(String s1 : pg.simMatchSet.get(e.from_node))
				for(String s2 : pg.simMatchSet.get(e.to_node)){
					Node n1 = dg.GetVertex(Long.valueOf(s1));
					Node n2 = dg.GetVertex(Long.valueOf(s2));
					if(dg.checkDistance(n1, n2))
						edgeCoverCount ++;
				}
		}
		
//		return ((matchAllNode.size() + edgeCoverCount) / (double)dgGraphSize );
		return (double)(matchAllNode.size() + edgeCoverCount);
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		int mode = 2;
		int time = 1;
		Vector<String> nodeMatchTest = new Vector<String>();
		if(args.length < 2)
			System.out.println("Input query graph and data graph");
		String filename1 = "/Users/qsong/Downloads/Simulation_test/test.db";
		String filename2 = "/Users/qsong/Downloads/Simulation_test/YagoCores_graph_small_sample1.db";
		for (int i = 0; i < time; i++) {
			pg = new gpm_graph();
			pg.ConstructGraphFromNeo4j(filename1, mode);
			dg = new Neo4jGraph(filename2,2);
			try(Transaction tx1 = dg.getGDB().beginTx() )
			{				
				Label_Index l = new Label_Index(dg);
				l.buildLabelIndex(false);
				IncJoinSimulation gsim = new IncJoinSimulation(pg, dg, true,l);
				System.out.println("interestingness: " + gsim.IsBSim(mode, true));
				gsim.displayMatch();
				tx1.success();
			}
		}
	}
}

