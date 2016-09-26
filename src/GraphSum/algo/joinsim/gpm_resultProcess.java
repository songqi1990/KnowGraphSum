package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DirectedMultigraph;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jGraph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.Neo4jSubGraph;
//import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;

import org.neo4j.graphdb.*;

//This funtion receives a pattern and a result graph,
//with a sequence of small graphs, ranked by importance function.

//first, the connected component of result graph is listed.
//then, for each connected component, subgraph isomorphism is 
//performed to list exact matchings.
public class gpm_resultProcess {

	public Neo4jGraph pg;//pattern 
	public Neo4jGraph dg;//data graph
	public DirectedMultigraph<Node,Relationship> rg;//result graph
	//public DirectedMultigraph<Node,Relationship> rg;
	public String prepath;

	public TreeMap<Long, Vector<Long>> matchSet;
	public List<DirectedMultigraph<Node	,Relationship>> conlist;
	public List<DirectedMultigraph<Node,Relationship>> refineconlist;
	public int matchsize;

	public gpm_resultProcess(Neo4jGraph patterng, Neo4jGraph datag, DirectedMultigraph<Node,Relationship> resultg, TreeMap<Long, Vector<Long>> mSet){
		pg = patterng;
		rg = resultg;
		dg = datag;
		prepath = pg.gfilename.substring(0, pg.gfilename.lastIndexOf("/")+1);
		matchSet = mSet;
		conlist = new ArrayList<DirectedMultigraph<Node,Relationship>>();
		refineconlist = new ArrayList<DirectedMultigraph<Node,Relationship>>();
	}
	
	//	============================================================================//
	//	This function defines comparator for Nodes
	//	============================================================================//
	public class neo4jSubgraphComparator implements Comparator<Neo4jSubGraph>{
		@Override
		public int compare(Neo4jSubGraph ga, Neo4jSubGraph gb){
			
			int inta = 0;
			int intb = 0;
			for(Relationship e : ga.getEdges()){
				if (e.hasProperty("bound")) {
					inta+=(int)e.getProperty("bound");
				} else {
					inta += 1;
				}
			}
			for(Relationship e : gb.getEdges()){
				if (e.hasProperty("bound")) {
					intb+=(int)e.getProperty("bound");
				} else {
					intb += 1;
				}
			}
			
			inta = inta/ga.getNodes().size();
			intb = intb/gb.getNodes().size();
			
			if (inta < intb){
				return -1;
			}
			if (inta > intb){
				return 1;
			}
			return 0;
		}
	}

//	============================================================================//
	//	This function defines comparator for gpm_nodes
	//	============================================================================//
	public class directgraphComparator implements Comparator<DirectedMultigraph<Node, Relationship>>{
		@Override
		public int compare(DirectedMultigraph<Node, Relationship> ga, DirectedMultigraph<Node, Relationship> gb){
			
			int inta = 0;
			int intb = 0;
			for(Relationship e : ga.edgeSet()){
				if (e.hasProperty("bound")) {
					inta+=(int)e.getProperty("bound");
				} else {
					inta += 1;
				}
			}
			for(Relationship e : gb.edgeSet()){
				if (e.hasProperty("bound")) {
					intb+=(int)e.getProperty("bound");
				} else {
					intb += 1;
				}
			}
			
			inta = inta/ga.vertexSet().size();
			intb = intb/gb.vertexSet().size();
			
			if (inta < intb){
				return -1;
			}
			if (inta > intb){
				return 1;
			}
			return 0;
		}
	}
	
	//this function lists all connected components of the result graph
	public void concomList(){
		//conlist = new ArrayList<DirectedMultigraph<Node,Relationship>>();
		ConnectivityInspector<Node,Relationship> conInsp = new ConnectivityInspector<Node,Relationship>(rg);
		System.out.println("Calculating CC");
		List<Set<Node>> cnodeset = conInsp.connectedSets();
		Vector<Set<Node>> cnodelist = new Vector<Set<Node>>();
		cnodelist.addAll(cnodeset);
		
		HashSet<Long> nlabels = new HashSet<Long>();
		Vector<Long> mset =  new Vector<Long>();
		//filter.
		/*
		for(int i=0;i<cnodelist.size();i++){
			boolean rmv = false;
			for(Node n: pg.getGGO().getAllNodes()){
				mset.clear();
				if(matchSet.get(n.getId()) != null)
					mset.addAll(matchSet.get(n.getId()));
				nlabels.clear();
				for(Node s:cnodelist.elementAt(i))
					nlabels.add(s.getId());
				mset.retainAll(nlabels);
				if(mset.size()==0){
					rmv = true;
					break;
				}
			}
			if(rmv){
				cnodelist.remove(i);
				i--;
			}
		}
		
		cnodeset.clear();
		cnodeset.addAll(cnodelist);
		*/
		if(cnodeset.size()>100){
			System.out.println("top 100");
		}
		/*
		for(int i=0;i<Math.min(cnodeset.size(), 100);i++){
			conlist.add(subRes(cnodeset.get(i), rg));
		}
		*/
		conlist.add(rg);
		Comparator<DirectedMultigraph<Node,Relationship>> comparator = new directgraphComparator();
		Collections.sort(conlist, comparator);
		matchsize = conlist.size();
		
		//return conlist;
	}
	
	
	
	//This function generates subgraphs from a directed multiple graph given a set of nodes
		public DirectedMultigraph<Node, Relationship> subRes(Set<Node> nset, DirectedMultigraph<Node, Relationship> g){
			DirectedMultigraph<Node, Relationship> nsub = new DirectedMultigraph<Node, Relationship>(Relationship.class);
			System.out.println("csize:"+ nset.size());
			for(Node n: nset){
				nsub.addVertex(n);
			}
			for(Node a: nset){
				for(Node b:nset){
					if(g.getEdge(a, b)!=null){
						Set<Relationship> eset = g.getAllEdges(a, b);
						for(Relationship e: eset)
							nsub.addEdge(a, b, e);
					}
				}
			}
			return nsub;
		}
	//storing in Neo4j

	//This function stores a multigraph.
	public void storemultig(String gfilename, Neo4jSubGraph rsub) throws IOException{
		System.out.println(gfilename);

		//store graph structure
		FileWriter fw = new FileWriter(gfilename);
		PrintWriter pw = new PrintWriter(fw);

		//node schema
		int i = 0;
		for(String attname: pg.schema.keySet()){
			String atp = attname+":"+pg.schema.get(attname);
			if(i!=0) atp = "	"+atp;
			pw.print(atp);
			i++;
		}
		pw.println();

		//edge schema
		i = 0;
		for(String rname: dg.relschema){
			if(i!=0) rname = "	"+rname;
			pw.print(rname);
			i++;
		}
		pw.println();

		//graph type
		pw.println("subgraph");

		//node set info
		pw.println(rsub.getNodes().size());
		for(Node n: rsub.getNodes()){
			pw.print(n.getId()+"\t");
			if (n.hasProperty("weight")) {
				pw.print( n.getProperty("weight")+"	");
			} else {
				pw.println("Node doesn't have weight.  ");
			}
					
			pw.print(Neo4jGraph.IterToSet(n.getPropertyKeys()).size());
			for(String s:n.getPropertyKeys()){
				pw.print("	"+s+(n.getProperty(s)));
			}
			pw.println();
		}

		//edge set info
		//HashSet<Relationship> eset = (HashSet);
		pw.println(rsub.getEdges().size());

		for(Relationship e:rsub.getEdges()){
		}

		pw.close();
		fw.close();
	}

	//This function stores a sequence of directed multigraphs.
	public void storeResList() throws IOException{
		String prefix = pg.gfilename;
		String path = prefix.substring(0, prefix.lastIndexOf("/")+1);
		prefix = prefix.substring(prefix.lastIndexOf("/")+1, prefix.length());
		int idx = 0;
		for(DirectedMultigraph<Node,Relationship> gl:conlist){
			System.out.println(path+"Match_"+idx+"_"+prefix);
			storemultig(path+"Match_"+idx+"_"+prefix, gl); 
			idx++;
		}
	}
	
	public void storemultig(String gfilename, DirectedMultigraph<Node, Relationship> rsub) throws IOException{
		//System.out.println(gfilename);

		//store graph structure
		FileWriter fw = new FileWriter(gfilename);
		PrintWriter pw = new PrintWriter(fw);



		//graph type
		//pw.println(dg.gtype);

		//node set info
		pw.println(rsub.vertexSet().size());
		for(Node n: rsub.vertexSet()){
			pw.print(n.getId()+"	");
			for(String s:n.getPropertyKeys()){
				pw.print("	"+s + "  " +n.getProperty(s));
			}
			pw.println();
		}

		//edge set info
		//HashSet<gpm_edge> eset = (HashSet);
		pw.println(rsub.edgeSet().size());

		for(Relationship e:rsub.edgeSet()){
			pw.println(e.getStartNode().getId()+"	"+e.getEndNode().getId()+"	");
		}

		pw.close();
		fw.close();
	}


}
