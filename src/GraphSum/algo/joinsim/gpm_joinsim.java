//this simulation is used for pattern selection algorithm,
//input is two gpm_graph, output is a set of matched edges
package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.IOException;
//import java.util.Collections;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Vector;

import org.jgrapht.graph.DirectedMultigraph;
import org.jgrapht.graph.DirectedWeightedMultigraph;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_topSort;


//This class computes the bounded simulation 
//between pattern and data graphs.

public class gpm_joinsim {

	public gpm_graph pg;
	public gpm_graph dg;
	//	public int mode;

	public TreeMap<String, Vector<String>> matchSet;
	public TreeMap<String, Vector<String>> canSet;
	public HashSet<String> matchingset;
	public HashSet<String> candidateset;
	public HashSet<String> reducedmatchingset;
	
	public int idxmode;



	public gpm_joinsim(gpm_graph P, gpm_graph G){
		pg = P;
		dg = G;
		matchSet = new TreeMap<String, Vector<String>>();
		matchingset = new HashSet<String>();
		reducedmatchingset = new HashSet<String>();
		candidateset = new HashSet<String>();
		canSet = new TreeMap<String, Vector<String>>();
	}


	//	============================================================================//
	//	display result  (matchSet)
	//	============================================================================//
	public void displayMatch(){

		System.out.println("Displaying Match:");
		System.out.println(this.pg.gfilename);
		System.out.println(this.dg.gfilename);
		System.out.println(matchSet.keySet().size());
		for(String pn: matchSet.keySet()){
			System.out.print(pn+":");
			for(String mn: matchSet.get(pn)){
				System.out.print(mn+" ");
			}
		}
	}

	//	============================================================================//	
	//	initialize sim candidate set
	//	============================================================================//
	public boolean Init() throws IOException{
		for(gpm_node pn: pg.vertexSet()){
			HashSet<String> ans = new HashSet<String>();
			for(gpm_node dn:dg.vertexSet()){
				if(dn.nlabel.equals(pn.nlabel))
					ans.add(dn.getTag());
			}
			if(ans==null||ans.size()==0){
				System.out.println("empty sim set.");
				return false;
			}
			Vector<String> ansvec = new Vector<String>();
			Vector<String> canvec = new Vector<String>();
			ansvec.addAll(ans);
			matchSet.put(pn.tag, ansvec);
			canSet.put(pn.tag, canvec);
		}
		return true;
	}

	//	============================================================================//
	//	This function performs the sim join for two pattern nodes
	// 	idx mode: 1: matrix; 2: distance buffer
	//	If change: return 1;
	//	If no change: return 0;
	//	If reduce to zero: return -1; (not bsim)
	//	============================================================================//

	public int simJoin(gpm_node a, gpm_node b){
		Vector<String> mseta = matchSet.get(a.tag);
		Vector<String> msetb = matchSet.get(b.tag);
		Vector<String> canseta = canSet.get(a.tag);
//		System.out.println("Join: "+mseta.size()+" X "+msetb.size());
		boolean change = false;
		boolean remv = true;

		for(int i=0;i<mseta.size();i++){
			remv = true;
			for(String pbsim: msetb){
				if(dg.checkConnection(dg.GetVertex(mseta.elementAt(i)), dg.GetVertex(pbsim))) {
					remv = false;
					break;
				}
			}
			if(remv){
				canseta.add(mseta.elementAt(i));
				mseta.remove(i);
				i--;
				change = true;
				if(mseta.size()==0)
					return -1;
			}
		}
		matchSet.put(a.tag, mseta);
		canSet.put(a.tag, canseta);
		if(change)
			return 1;
		return 0;
	}

	//	============================================================================//
	//	This function defines comparator for gpm_nodes
	//	============================================================================//
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

	//	============================================================================//	
	//	main process of bounded sim checking -- with join over sim sets
	//	============================================================================//
	@SuppressWarnings({ })
	public Vector<gpm_edge> IsBSim(boolean opt) throws IOException{

		Vector<gpm_edge> edgeResult = new Vector<gpm_edge>();
		//step 1: init. return false if for some node, init sim set is empty.
		Boolean aaa = Init();
		if(!aaa){
			return null;
		}

		//step 2: topsorting and ranking
		gpm_topSort gsort = new gpm_topSort(pg);
		Vector<gpm_node> sccnodes = gsort.sortedsccnodes;
		if(!opt){
			Collections.shuffle(sccnodes);
		}

		//step 3: sim join with topological order
		//the nodes with larger rank (low topological order) is processed first.

		Vector<gpm_node> nset = new Vector<gpm_node>();
		int result = 0;
		
		if(opt){
			for(gpm_node abpn:sccnodes){
//				System.out.println("SccNodes: "+abpn.tag+" "+abpn.weight+" "+abpn.addinfo);
				nset.clear();
				nset.addAll(pg.expSCC(abpn));

				//displayMatch();

				Comparator<gpm_node> comparator = new simsetComparator();
				Collections.sort(nset, comparator);

				boolean change = true;
				//scc inner join
				while(change){
					change = false;
					for(gpm_node a: nset){
						//					System.out.println(a.tag);
						Vector<gpm_node> pvec = pg.GetParents(a, true, -1);
						//					System.out.println(pvec);
						for(gpm_node pa: pg.GetParents(a, true, -1)){
//							System.out.println("edge: "+pa.tag +"-"+a.tag);
							result = simJoin(pa, a);
							if(result==1)
								change = true;
							else if(result==0)
								change = false;
							else if(result==-1){
								return null;
							}
						}
					}
				}
			}
		}
		//step 4: collect result
		for(gpm_edge e : pg.edgeSet()){
			for(String s1 : matchSet.get(e.from_node))
				for(String s2 : matchSet.get(e.to_node)){
					if(dg.checkConnection(dg.GetVertex(s1), dg.GetVertex(s2)))
						edgeResult.add(dg.GetEdge(s1, s2));
				}
		}
		return edgeResult;
	}


	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
	}
}

