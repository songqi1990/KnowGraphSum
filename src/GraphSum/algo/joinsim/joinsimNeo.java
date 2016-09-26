package wsu.eecs.mlkd.KGQuery.algo.joinsim;


import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Vector;

import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_edge;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;

import org.neo4j.graphdb.*;

//This class computes the bounded simulation 
//between pattern and data graphs.

public class joinsimNeo {
	public static gpm_graph pg;
	public static Neo4jGraph dg;

	public TreeMap<String, Vector<String>> matchSet;
	public TreeMap<String, Vector<String>> canSet;
	public Property_Index propertyIndex;
	public Label_Index labelIndex;
	public HashSet<String> matchingset;
	public HashSet<String> candidateset;
	public HashSet<String> reducedmatchingset;

	public int idxmode;
	public int realAFF1 = 0;// those really possible affects matches.

	public long distquerybfs = 0;
	public long distquerybibfs = 0;

	// this opt is for distance buffer checking.
	// if set to true,
	// the checking of distance buffer is under optimization mode.
	public boolean checkopt = true;

	// this vector saves all results, including:
	// 0. attribute index time;
	// 1. distance index time;
	// 2. running time after index;
	// 3. result truth value;
	// 4. min match size;
	// 5. max match size;
	// 6. average match size;
	// 7. result min match size;
	// 8. result max match size;
	// 9. result aver match size;
	public Vector<Long> resVec;


	public joinsimNeo(gpm_graph P, Neo4jGraph G, boolean opt) {

		matchSet = new TreeMap<String, Vector<String>>();
		matchingset = new HashSet<String>();
		reducedmatchingset = new HashSet<String>();
		candidateset = new HashSet<String>();
		propertyIndex = new Property_Index(G);
		labelIndex = new Label_Index(G);
		canSet = new TreeMap<String, Vector<String>>();
		resVec = new Vector<Long>(10);
		for (int i = 0; i < 10; i++)
			resVec.add(null);
		checkopt = opt;
	}



	// ============================================================================//
	// display result
	// ============================================================================//
	public void displayMatch() {

		System.out.println("Displaying Match:");
		System.out.println(this.pg.gfilename);
		System.out.println(this.dg.gfilename);

		for (String pn : matchSet.keySet()) {
			System.out.print(pn + ":");
			for (String mn : matchSet.get(pn)) {
				System.out.print(mn + " ");
			}
			System.out.println();
		}
	}

	public void buildPropertyIdx() throws IOException{
		long time = System.currentTimeMillis();
		if(propertyIndex.proIndex.size()==0){
			propertyIndex.bulidPropertyIndex(true);
		}
		System.out.println("Property index build success");
		resVec.set(0, System.currentTimeMillis() - time);
	}
	
	public void buildLabelIdx() throws IOException{
		long time = System.currentTimeMillis();
		if(labelIndex.invIndex.size()==0){
			labelIndex.buildLabelIndex(false);
		}
		System.out.println("Property index build success");
		resVec.set(0, System.currentTimeMillis() - time);
	}
	
	// ============================================================================//
	// initialize sim candidate set
	// ============================================================================//
	public boolean Init() throws IOException {
		int mins = Integer.MAX_VALUE;
		int maxs = -1;
		int aver = 0;
		HashSet<String> a1 = new HashSet<String>();
		HashSet<String> a2 = new HashSet<String>();
		HashSet<String> a3 = new HashSet<String>();
		buildLabelIdx();
//		System.out.println((pg == null) + " pg null check");
		
		long st1 = System.currentTimeMillis();
		
		//Vector<AVpair> avlist = null;
		for (gpm_node pn : pg.vertexSet()) {
			//avlist = pn.getAVlist();
			System.out.println("generating ans set for node " + pn.tag);
			HashSet<String> ans = new HashSet<String>();
			ans.addAll(labelIndex.getLabelAnswer(pn.nlabel));
			System.out.println("Candidate set for " + pn.tag + ": " + ans.size());
			if (ans == null || ans.size() == 0) {
				System.out.println("empty sim set.");
				resVec.set(4, (long) mins);
				resVec.set(5, (long) maxs);
				resVec.set(6, (long) (maxs + mins) / 2);
				return false;
			}
			Vector<String> ansvec = new Vector<String>();
			Vector<String> canvec = new Vector<String>();
			ansvec.addAll(ans);
			if (ans.size() < mins)
				mins = ans.size();
			if (ans.size() > maxs)
				maxs = ans.size();
			aver += ans.size();
			matchSet.put(pn.tag, ansvec);
			canSet.put(pn.tag, canvec);
			
		}	
		
		long st2 = System.currentTimeMillis();
		System.out.println("Time spend on canSet: " + (st2 - st1));
		//System.out.println("***************");
		//System.out.println(matchSet);
		//System.out.println(canSet);
		//System.out.println("***************");
		aver = aver / (matchSet.keySet().size());
		resVec.set(4, (long) mins);
		resVec.set(5, (long) maxs);
		resVec.set(6, (long) aver);
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
		System.out.println("Join: "+mseta.size()+" X "+msetb.size());
		//HashSet<String> cseta = new HashSet<String>();
		//cseta.addAll(canseta);
		boolean change = false;
		boolean remv = true;

		for (int i = 0; i < mseta.size(); i++) {
			remv = true;
			for (String pbsim : msetb) {
				//if (dg.checkDistance(checkopt, dg.GetVertex(Long.valueOf(mseta.elementAt(i))),
					//	dg.GetVertex(Long.valueOf(pbsim)), bound, idxmode, color, false)) {
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
		System.out.println("After join: " + mseta.size());
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
		int aversize = 0;
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
			aversize += size;
		}

		for (String s : canSet.keySet()) {
			Vector<String> cset = matchSet.get(s);

			for (String c : cset) {
				candidateset.add(s + ":" + c);
			}
		}
		aversize = aversize / matchSet.keySet().size();

		resVec.set(7, (long) minsize);
		resVec.set(8, (long) maxsize);
		resVec.set(9, (long) aversize);

	}

	// this function stores the match results
	public void storeMatchResult() throws IOException {
		String pname = pg.gfilename;
		String path = pname.substring(0, pname.lastIndexOf("/") + 1);
		pname = pname.substring(pname.lastIndexOf("/") + 1, pname.length());
		String gname = dg.gfilename;
		gname = gname.substring(gname.lastIndexOf("/") + 1, gname.length());
		String matchname = path + "join_Match_" + pname + "_" + gname;

		FileWriter fw = new FileWriter(matchname);
		PrintWriter pw = new PrintWriter(fw);

		pw.println(pg.vertexSet().size());
		for (String s : matchSet.keySet()) {
			pw.print(s + ":");
			Vector<String> mset = matchSet.get(s);
			for (String ms : mset) {
				pw.print(ms + "	");
			}
			pw.println();
		}

		pw.close();
		fw.close();
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
	public Vector<Long> IsBSim(int idxmode, boolean opt) throws IOException {
		this.idxmode = idxmode;
		long time = System.currentTimeMillis();
		long matrixtime = 0;

		long run = System.currentTimeMillis();
		// step 1: init. return false if for some node, init sim set is empty.
		if (!Init()) {
			resVec.set(1, matrixtime);
			resVec.set(2, System.currentTimeMillis() - run);
			resVec.set(3, (long) 0);
			resVec.set(7, (long) 0);
			resVec.set(8, (long) 0);
			resVec.set(9, (long) 0);
			return resVec;
		}

		long st3 = System.currentTimeMillis();
		
		// step 2: topsorting and ranking
		//
		gpm_topSort gsort = new gpm_topSort(pg);
		Vector<gpm_node> sccnodes = gsort.sortedsccnodes;
		if(!opt){
			Collections.shuffle(sccnodes);
		}
		

		// step 3: sim join with topological order
		// the nodes with larger rank (low topological order) is processed
		// first.

		System.out.println("enter former checking...");
		Vector<gpm_node> nset = new Vector<gpm_node>();
		int result = 0;

		if (opt) {
			for ( gpm_node abpn : sccnodes) {
				//System.out.println("SccNodes: " + abpn.getId() + " "
				//		+ abpn.getProperty("weight") + " " + abpn.getProperty("info"));
				nset.clear();
				nset.addAll(pg.expSCC(abpn));

				// displayMatch();

				Comparator<gpm_node> comparator = new simsetComparator();
				Collections.sort(nset, comparator);

				boolean change = true;
				// scc inner join
				while (change) {
					change = false;
					for (gpm_node a : nset) {
						Vector<gpm_node> pvec = pg.GetParents(a, true, -1);
						for (gpm_node pa : pg.GetParents(a, true, -1)) {
							System.out.println(
									"edge: " + pa.tag + "-" + a.tag);
							result = simJoin(pa, a);
							if (result == 1)
								change = true;
							else if (result == 0)
								change = false;
							else if (result == -1) {
								resVec.set(1, matrixtime);
								resVec.set(2, System.currentTimeMillis() - run);
								resVec.set(3, (long) 0);
								resVec.set(7, (long) 0);
								resVec.set(8, (long) 0);
								resVec.set(9, (long) 0);
								return resVec;
							}
						}
					}
				}
			}
		}

		// step 4: collect result
		// displayMatch();
		resVec.set(1, matrixtime);
		resVec.set(2, System.currentTimeMillis() - run);
		resVec.set(3, (long) 1);
		msize();
		// matchse found.
		//System.out.println("matchSet = " + matchSet);
		
		long st4 = System.currentTimeMillis();
		System.out.println("Time spend on joinSim: " + (st4 - st3));
		
		if (resVec.elementAt(3) == 1) {

			System.out.println("Dist Time (BiBFS): " + dg.distQtime
					+ " Dist Time 2 (BFS): " + dg.distQtime2);
			System.out.println(resVec);
			processMatch();
			supportCalculate();
			coverageCalculate();
		}
		
		return resVec;
	}

	public void coverageCalculate(){
		Vector<String> matchAllNode = new Vector<String>();
		for (String pn : matchSet.keySet()){
			for (String mn : matchSet.get(pn)) {
				if(!matchAllNode.contains(mn) && !reducedmatchingset.contains(mn))
					matchAllNode.addElement(mn);
			}
		}
		System.out.println("coverage = " + (float)matchAllNode.size()/dg.getNodeNumber());
	}
	
	public void supportCalculate(){
		Vector<String> matchEachNode = new Vector<String>();
		int support = 0;
		for (String pn : matchSet.keySet()){
			matchEachNode.clear();
			for (String mn : matchSet.get(pn)) {
				if(!matchEachNode.contains(mn) && !reducedmatchingset.contains(mn))
					matchEachNode.addElement(mn);
			}
			System.out.println("matchEachNode size: " + matchEachNode.size());
			if(support == 0)
				support = matchEachNode.size();
			else
				support = matchEachNode.size() < support ? matchEachNode.size(): support;
		}
		System.out.println("support = " + support);
	}
	
	
	//postprocess on matchings.
		public void processMatch() throws IOException{
			int edgeMatchNumber = 0;
			boolean change = true;
			int reducenode = 0;
			System.out.println("Processing Results..");
			DirectedMultigraph<Node	,Relationship> resg = match2Graph();
			System.out.println("V size: "+ resg.vertexSet().size()+ " E size: "+ resg.edgeSet().size());
			
			for(Node n : resg.vertexSet())
				System.out.println("V: " + n.getId());
			for(Relationship r : resg.edgeSet())
				System.out.println("E: " + r.getStartNode().getId() + "---" + r.getEndNode().getId());
			reducedmatchingset = new HashSet<String>();
			while (change == true)
			{
				change = false;
				for(gpm_edge e: pg.edgeSet()){
					Vector<String> amset = matchSet.get(e.from_node);
					Vector<String> bmset = matchSet.get(e.to_node);
					if(amset == null || bmset == null)
						continue;
					for(String am: amset){
						if(reducedmatchingset.contains(am))
							continue;
						edgeMatchNumber = 0;
						//System.out.println("am :" + am);
						for(String bm: bmset){
							//System.out.println("bm :" + bm);
							if(resg.containsEdge(dg.GetVertex(Long.valueOf(am)), dg.GetVertex(Long.valueOf(bm)))){
								//reducedmatchingset.add(e.getStartNode() + ":"+am);
								//reducedmatchingset.add(e.getEndNode() + ":"+bm);
								edgeMatchNumber ++;
							}
						}
						//System.out.println("edgeMatchNumber :" + edgeMatchNumber);
						//if(edgeMatchNumber < pg.rangeL[(int)e.getId()] || edgeMatchNumber > pg.rangeU[(int)e.getId()]){
						if(edgeMatchNumber < 2 || edgeMatchNumber > 5){
							reducedmatchingset.add(am);
							change = true;
						}
					}		
				}
			}
			
			reducenode = reducedmatchingset.size();
			System.out.println("***********Reduced Nodes************");
			System.out.println(reducenode);
			System.out.println("************************************");
			
			//for(Long am : reducedmatchingset){
				//System.out.println(am);
			//}
			
			/*
			gpm_resultProcess proc = new gpm_resultProcess(pg,dg,resg,matchSet);
			System.out.println("complist..");
			proc.concomList();
			System.out.println("storing..");
			proc.storeResList();
			System.out.println("subgraphs:" + proc.matchsize);
			*/
		}

		//	============================================================================//	
		//	transform match sets to a graph
		//	============================================================================//
		public DirectedMultigraph<Node,Relationship> match2Graph(){
			DirectedMultigraph<Node,Relationship> msubg = new DirectedWeightedMultigraph<Node,Relationship>(Relationship.class);
			int bound=0;
			int color=0;
			for(String s: matchSet.keySet()){
				for(gpm_node sc: pg.GetChildren(pg.GetVertex(s), -1)){
					gpm_edge e = pg.GetEdge(s,sc.tag);
					bound = e.e_bound;
					color = e.e_color;
					Vector<String> ms1 = matchSet.get(s);
					Vector<String> ms2 = matchSet.get(sc.tag);
					if (ms1 != null && ms2 != null){
						for(String m1: ms1){
							for(String m2:ms2){
								Node a = dg.GetVertex(Long.valueOf(m1));
								Node b = dg.GetVertex(Long.valueOf(m2));
								Relationship r = dg.getRelationship(a, b);
								if(r!=null){
									msubg.addVertex(a);
									msubg.addVertex(b);
									msubg.addEdge(a, b, r);
								}
							}
						}
					}
				}
			}
			return msubg;
		}
	
		// ============================================================================//
	// This function creates a vector from a string
	// ============================================================================//
	public static Vector<String> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<String> vec = new Vector<String>();
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(words[i]);
		}
		return vec;
	}

	/**
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {

		int mode = 2;
		int time = 1;
		// long runtime = 0;
		long bibfs[] = new long[time];
		long bfs[] = new long[time];
		long dm[] = new long[time];

//		if(args.length < 2)
			System.out.println("Input query graph and data graph");
		String filename1 = args[0];
		String filename2 =args[1];
		for (int i = 0; i < time; i++) {
			// System.out.println("Round "+(i+1) + "...");
			// runtime = System.currentTimeMillis();
			pg = new gpm_graph();
			pg.ConstructGraphFromNeo4j(filename1, mode);;
			dg = new Neo4jGraph(filename2,2);
			// gd.distQtime = 0;
			// gd.distQtime2 = 0;
			// gd.distQtimeDM = 0;

				//System.out.println("pg: " + pg.getNodeNumber());
			try(Transaction tx1 = dg.getGDB().beginTx() )
			{					
				joinsimNeo gsim = new joinsimNeo(pg, dg, true);
				System.out.println(gsim.IsBSim(mode, true));
				System.out.println("total bibfs time: "
						+ dg.distQtime / 1000000 + " total bfs time: "
						+ dg.distQtime2 / 1000000);
				// totaltime+=System.currentTimeMillis() -
				// runtime;//gsim.resVec.elementAt(2);
				bibfs[i] = dg.distQtime / 1000000;
				bfs[i] = dg.distQtime2 / 1000000;
				dm[i] = dg.distQtimeDM / 1000000;
//				gsim.displayMatch();
				System.out.println("time:" + gsim.resVec.elementAt(2));
				tx1.success();
			}
		}
		for (int i = 0; i < time; i++) {
			System.out.println("" + (i + 1) + "	" + bibfs[i] + "	" + bfs[i]
					+ "	" + dm[i]);
		}

	}
}

