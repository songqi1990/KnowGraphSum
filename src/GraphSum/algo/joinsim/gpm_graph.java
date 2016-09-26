package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultDirectedWeightedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;

import wsu.eecs.mlkd.KGQuery.algo.SPMine.SPMine_filter;

public class gpm_graph extends DefaultDirectedWeightedGraph<gpm_node, gpm_edge>{

	public String gfilename = "";

	public int gtype = 0; // pattern: 0; datagraph: 1
	public int patternId = 0; //pattern Id, 0 for datagraph, >= 1 for pattern
	
	public HashMap<String, String> schema; // schema on vertex. format: attr:type

	public Vector<String> relschema;//schema on edges: ie, colors
	public TreeSet<Integer> edgeSetMark;//used to mark a graph, for pattern duplicate checking
	public int graphId;


	public double interestingness = 0.0;	//actually it's the support
	public StrongConnectivityInspector<gpm_node, gpm_edge> sccIns; 

	//stores the hash result for minHash
	public int[] hashResult;
	
	//Stores current match set for nodes
	public TreeMap<String, HashSet<String>> simMatchSet; 
	
	private static final long serialVersionUID = 1L;

	public gpm_graph() {
		super(gpm_edge.class);
	}

	//	============================================================================//
	//	This function casts gpm_graph into directedgraph class.  
	//	============================================================================//
	@SuppressWarnings("unchecked")
	public DirectedGraph Cast2DG(){ 
		DirectedGraph<gpm_node,gpm_edge> DG = 
			new DefaultDirectedGraph<gpm_node, gpm_edge>(gpm_edge.class);
			for(gpm_node n: this.vertexSet())
				DG.addVertex(n);
			for(gpm_edge e:this.edgeSet()){
				DG.addEdge(this.GetVertex(e.from_node),this.GetVertex((e.to_node)),e);
			}
			return DG;
	}

	//	============================================================================//
	//	This function inserts a user-defined node into graph 
	//	============================================================================//
	public boolean InsertNode(gpm_node n){
		//n.tag = ""+(this.vertexSet().size()+1);
		return(this.addVertex(n));
	}
	
	//	============================================================================//
	//	This function inserts a user-defined edge into graph  
	//	============================================================================//
	public boolean InsertEdge(gpm_edge e){
		if(e!=null){
			gpm_node a = this.GetVertex(e.from_node);
			gpm_node b = this.GetVertex(e.to_node);
			return(this.addEdge(a, b, e));
		}
		return false;
	}
	
//	============================================================================//
	//	This function inserts a user-defined edge into graph  
	//	============================================================================//
	public boolean InsertEdgeByLabel(gpm_edge e){
		if(e!=null){
			gpm_node a = this.GetVertex(e.from_node);
			gpm_node b = this.GetVertex(e.to_node);
			return(this.addEdge(a, b, e));
		}
		return false;
	}

	//	============================================================================//
	//	This function gets vertex wrt node tag
	//	============================================================================//
	public gpm_node GetVertex(String ntag){
		for(gpm_node n: this.vertexSet()){
			if(n.tag.equals(ntag))
				return n;
		}
		return null;
	}

	//	============================================================================//
	//	This function gets vertex wrt node tag
	//	============================================================================//
	public gpm_node GetVertexByLabel(String nlabel){
		for(gpm_node n: this.vertexSet()){
			if(n.nlabel.equals(nlabel))
				return n;
		}
		return null;
	}
	
	//	============================================================================//
	//	This function gets edge wrt source-sink node tags
	//	============================================================================//
	public gpm_edge GetEdge(String fid, String tid){
		gpm_node fn  = this.GetVertex(fid);
		gpm_node tn = this.GetVertex(tid);
		return (gpm_edge)this.getEdge(fn, tn);
	}

	//	============================================================================//
	//	This function gets edge wrt source-sink node tags
	//	============================================================================//
	public gpm_edge GetEdgeByLabel(String flabel, String tlabel){
		gpm_node fn  = this.GetVertexByLabel(flabel);
		gpm_node tn = this.GetVertexByLabel(tlabel);
		return (gpm_edge)this.getEdge(fn, tn);
	}
	
	//	============================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//  if color = -1, all colored edges will be added.
	//	============================================================================//
	public Vector<gpm_node> GetParents(gpm_node n, boolean sort, int color){
		Vector<gpm_node> pset = new Vector<gpm_node>();
		Vector<Integer> bound = new Vector<Integer>();
		for(gpm_edge e: this.incomingEdgesOf(n)){
			if(((gpm_edge)e).e_color==color){
				pset.add((gpm_node)this.getEdgeSource(e));
			}
			else if(color==-1)
				pset.add((gpm_node)this.getEdgeSource(e));
			if(sort){
				bound.add(e.e_bound);
				for(int i = bound.size()-1; i>0;i--){
					if(bound.elementAt(i).compareTo(bound.elementAt(i-1))>0){
						Integer tmp = bound.elementAt(i-1);
						bound.set(i-1, bound.elementAt(i));
						bound.set(i, tmp);
						gpm_node tmpn = pset.elementAt(i-1);
						pset.set(i-1, pset.elementAt(i));
						pset.set(i, tmpn);
					}
				}
			}
		}
		return pset;
	}


	//	============================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//	============================================================================//
	public HashSet<gpm_node> GetParentsSet(HashSet<gpm_node> nset,  int color){
		HashSet<gpm_node> psets = new HashSet<gpm_node>();
		//		HashSet<gpm_node> center = new HashSet<gpm_node>();
		//		center.addAll(nset);
		for(gpm_node n: nset){
			psets.addAll(GetParents(n,false,color));
		}
		return psets;
	}

	//	============================================================================//
	//	This function gets child set of a node 
	//	============================================================================//
	public Vector<gpm_node> GetChildren(gpm_node n, int color){
		Vector<gpm_node> cset = new Vector<gpm_node>();
		for(gpm_edge e:this.outgoingEdgesOf(n)){
			if(color!=-1 && ((gpm_edge)e).e_color==color){
				cset.add((gpm_node)this.getEdgeTarget(e));
			}
			else if(color==-1)
				cset.add((gpm_node)this.getEdgeTarget(e));
		}
		return cset;
	}

	//	============================================================================//
	//	This function clears the weights of nodes 
	//	============================================================================//
	public void ClearNodeWeight(){
		for(gpm_node n: this.vertexSet())
			n.weight = 0.0;
	}
	
	
	//	============================================================================//
	//	This function returns the DAG with each SCC a node.
	//  This function deal with small pattern graphs. 
	//	Each scc node has add info as "0_1_2_.._k" with 0-k the node id.
	//	This function also computes bisimulation ranks. See "the fast bisimulation algorithm"
	//	============================================================================//
	@SuppressWarnings("unchecked")
	public HashMap<Integer, HashSet<gpm_node>> sccDAGwithrank(){//Vector<HashSet<gpm_node>>

		Vector<HashSet<gpm_node>> C = new Vector<HashSet<gpm_node>>();
		HashMap<Integer, HashSet<gpm_node>> rankmap = new HashMap<Integer, HashSet<gpm_node>>();

		DirectedGraph dg = this.Cast2DG();
		gpm_graph sccDAG = new gpm_graph();
		if(sccIns==null)
			sccIns = new StrongConnectivityInspector(dg);
		List<Set<gpm_node>> sccsets = sccIns.stronglyConnectedSets();


		HashSet<gpm_node> WF = new HashSet<gpm_node>();
		HashSet<gpm_node> NWF = new HashSet<gpm_node>();

		for(int i=0;i<sccsets.size();i++){

			Set<gpm_node> s = sccsets.get(i);

			for(gpm_node n: s){
				n.addinfo = ""+i;
			}

			gpm_node scc = new gpm_node();
			for(gpm_node n: s){
				scc.addinfo = scc.addinfo + n.tag + "_";
			}
			scc.weight = s.size();
			scc.addinfo = scc.addinfo.substring(0,scc.addinfo.lastIndexOf("_"));
			scc.tag = ""+i;
			sccDAG.addVertex(scc);

			if(sccsets.get(i).size()>1)
				NWF.add(scc);
		}


		for(gpm_edge e: this.edgeSet()){
			String src = this.getEdgeSource(e).addinfo;
			String trg = this.getEdgeTarget(e).addinfo;
			if(!src.equals(trg)){
				gpm_edge he = new gpm_edge(src,trg,1,1);
				sccDAG.InsertEdge(he);
			}
		}

		//propagate nonWF area
		HashSet<gpm_node> newnf = sccDAG.GetParentsSet(NWF, -1);
		HashSet<gpm_node> tmp = new HashSet<gpm_node>();
		tmp.addAll(newnf);

		while(newnf.size()> 0){
			NWF.addAll(newnf);
			newnf.clear();
			newnf.addAll(sccDAG.GetParentsSet(tmp, -1));
			newnf.removeAll(NWF);
			tmp.clear();
			tmp.addAll(newnf);
		}

		WF.addAll(sccDAG.vertexSet());
		WF.removeAll(NWF);


		//the following is for whether a ranking based on sccgraph is required
		List<gpm_node> nlist = new ArrayList<gpm_node>();

		TopologicalOrderIterator topiter = 
			new TopologicalOrderIterator(sccDAG);

		while(topiter.hasNext()){
			gpm_node n = (gpm_node) topiter.next();
			nlist.add(n);
		}

		//bottom up
		Collections.reverse(nlist);

		HashSet<gpm_node> col = new HashSet<gpm_node>();
		Vector<gpm_node> Cset = new Vector<gpm_node>();
		HashSet<gpm_node> tmprset = new HashSet<gpm_node>(); 

		for(gpm_node n: nlist){
			col.clear();
			col.addAll(this.expSCCwithaddinfo(n));
			if(sccDAG.outDegreeOf(n)==0 && n.weight==1){
				n.rank = 0;
				for(gpm_node v:col) v.rank = 0;
			}
			else if(sccDAG.outDegreeOf(n)==0 && n.weight>1){
				n.rank = Integer.MIN_VALUE;
				for(gpm_node v:col) v.rank = Integer.MIN_VALUE;
			}
			else{
				Cset.clear();
				Cset = sccDAG.GetChildren(n, -1);
				n.rank = Integer.MIN_VALUE;
				for(gpm_node cn: Cset){
					if(WF.contains(cn) && (cn.rank+1)> n.rank)
						n.rank = cn.rank+1;
					else if(NWF.contains(cn) && cn.rank>n.rank && cn.rank>=0)
						n.rank = cn.rank;
					else if(NWF.contains(cn) && cn.rank>=n.rank && cn.rank<0)
						n.rank = Integer.MIN_VALUE;
				}
				for(gpm_node v:col) v.rank = n.rank;
			}

			tmprset.clear();
			if(rankmap.get(n.rank)!=null){
				tmprset.addAll(rankmap.get(n.rank));
				tmprset.addAll(col);
				rankmap.put(n.rank, new HashSet<gpm_node>(tmprset));
			}
			else rankmap.put(n.rank, new HashSet<gpm_node>(col));
		}
		return rankmap;//C;
	}

	@SuppressWarnings("unchecked")
	public gpm_graph sccDAG(){
		DirectedGraph dg = this.Cast2DG(); 
		gpm_graph sccDAG = new gpm_graph();
		if(sccIns==null)
			sccIns = new StrongConnectivityInspector(dg);
		List<Set<gpm_node>> sccsets = sccIns.stronglyConnectedSets();

		HashSet<gpm_node> WF = new HashSet<gpm_node>();
		HashSet<gpm_node> NWF = new HashSet<gpm_node>();

		Vector<gpm_node> sccnodes = new Vector<gpm_node>();
		HashSet<gpm_edge> sccedges = new HashSet<gpm_edge>();

		for(int i=0;i<sccsets.size();i++){
			Set<gpm_node> s = sccsets.get(i);

			for(gpm_node n: s){
				n.addinfo = ""+i;
			}

			gpm_node scc = new gpm_node();
			for(gpm_node n: s){
				scc.addinfo = scc.addinfo + n.tag + "_";
			}
			scc.tag = scc.addinfo.substring(0,scc.addinfo.lastIndexOf("_"));
			sccnodes.add(scc);
			if(sccsets.get(i).size()>1)
				NWF.addAll(sccsets.get(i));
		}

		//propagate nonWF area
		HashSet<gpm_node> newnf = this.GetParentsSet(NWF, -1);
		HashSet<gpm_node> tmp = new HashSet<gpm_node>();
		tmp.addAll(newnf);

		while(newnf.size()> 0){
			NWF.addAll(newnf);
			newnf.clear();
			newnf.addAll(this.GetParentsSet(tmp, -1));
			newnf.removeAll(NWF);
			tmp.clear();
			tmp.addAll(newnf);
		}

		WF.addAll(this.vertexSet());
		WF.removeAll(NWF);

		//computing ranks
		for(int i=0;i<sccsets.size();i++){
			Set<gpm_node> s = sccsets.get(i);

			for(int j=0;j<i && j!=i;j++){
				Set<gpm_node> s2 = sccsets.get(j);
				for(gpm_node na: s){
					for(gpm_node nb: s2){
						if(this.getEdge(na, nb)!=null){
							gpm_edge e = new gpm_edge(sccnodes.elementAt(i).tag,sccnodes.elementAt(j).tag,1,0);
							sccedges.add(e);
							break;
						}
						else if(this.getEdge(nb, na)!=null){
							gpm_edge e = new gpm_edge(sccnodes.elementAt(j).tag,sccnodes.elementAt(i).tag,1,0);
							sccedges.add(e);
							break;
						}
					}
					break;
				}
			}
		}

		String sccname = gfilename.substring(0, gfilename.indexOf("."));
		sccname = sccname + "_sccDAG.grp";
		sccDAG.ConstructGraphFromVec(0, sccname, this.schema, sccnodes, sccedges, this.relschema);

		return sccDAG;
	}

	//	============================================================================//
	//	This function expands an scc node into a node set
	//	============================================================================//
	public HashSet<gpm_node> expSCC(gpm_node sccnode){
		String s = sccnode.tag;
		Vector<String> nids = createVectorFromString(s,"_");
		HashSet<gpm_node> nset = new HashSet<gpm_node>();
		for(String id: nids){
			nset.add(this.GetVertex(id));
		}
		return nset;
	}

	//	============================================================================//
	//	This function checks the connection of two vertex.
	//	============================================================================//
	public boolean checkConnection(gpm_node a, gpm_node b){
		if(this.getEdge(a, b)==null)
			return false;
		else
			return true;
	}

	//	============================================================================//
	//	This function expands an scc node into a node set
	//	============================================================================//
	public HashSet<gpm_node> expSCCwithaddinfo(gpm_node sccnode){
		String s = sccnode.addinfo;
		Vector<String> nids = createVectorFromString(s,"_");
		HashSet<gpm_node> nset = new HashSet<gpm_node>();
		for(String id: nids){
			nset.add(this.GetVertex(id));
		}
		return nset;
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
	//	This function constructs a graph from a list of nodes and edges 
	//	============================================================================//
	public void ConstructGraphFromVec(int type, String gfname, HashMap<String, String> schma, Vector<gpm_node> vlist,HashSet<gpm_edge> elist, Vector<String> ecolor){
		this.gtype = type;
		this.gfilename = gfname;
		this.schema = schma;
		if(this.relschema==null)
			this.relschema = new Vector<String>();
		this.relschema = ecolor;
		for(gpm_node n: vlist){
			this.InsertNode(n);
		}
		for(gpm_edge e:elist){
			this.InsertEdge(e);
		}
	}

//	============================================================================//
	//	This function constructs a graph from neo4j 
	//	============================================================================//
	public void ConstructGraphFromNeo4j(String graphDbPath, int idxmode){
		GraphDatabaseService graphDb;
		this.gfilename = graphDbPath;
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( graphDbPath );
		String ntag = "";
		double nweight = 0.0;
		String nlabel = "";
		Vector<AVpair> alist = new Vector<AVpair>();
		this.simMatchSet = new TreeMap<String, HashSet<String>>();
		HashSet<String> nodeMatch = new HashSet<String>();
		try ( Transaction tx = graphDb.beginTx() )
		{
			GlobalGraphOperations globalOperation =  GlobalGraphOperations.at(graphDb);
			
			Iterable<String> propertyKeys  = globalOperation.getAllPropertyKeys();
			if(this.schema==null)
				this.schema = new HashMap<String, String>();
			for(String kitem: propertyKeys){
				String attname = kitem;
				String type = "String";
				this.schema.put(attname, type);
			}
			if(this.relschema==null)
				this.relschema = new Vector<String>();
			this.relschema.add("0");
			
			Iterable<Node> neoNodes  = globalOperation.getAllNodes();
			for ( Node item : neoNodes )
			{
				alist.clear();
				ntag = Long.toString(item.getId());
				nweight = 0.0;
				Iterable<String> nodePropertyKeys = item.getPropertyKeys();
				for (String kitem : nodePropertyKeys)
				{
					String att="", op="",val="";
					att = kitem;
					op = "=";
					val = item.getProperty(kitem).toString();
					alist.add(new AVpair(att,op,val));
				}
				for (Label klabel : item.getLabels()){
					nlabel = klabel.name();
				}
				gpm_node v = new gpm_node(alist,ntag,nweight,nlabel);
				this.addVertex(v);
				this.simMatchSet.put(v.tag, nodeMatch);
			}
			
			Iterable<Relationship> neoRelationships = globalOperation.getAllRelationships();
			for (Relationship item : neoRelationships)
			{
				gpm_edge e = new gpm_edge(Long.toString(item.getStartNode().getId()),Long.toString(item.getEndNode().getId()),1,0);
				this.InsertEdge(e);
			}
			tx.success();
		}
	}
	
	//	============================================================================//
	//	This function constructs a graph from a given pattern graph
	//  This function can be used in two ways: 
	//  1. to duplicate redundant queries. for this, use exactcopyPN.
	//  2. to generate synthetic data graphs. for this, use copyPN.
	//	============================================================================//
	public void ConstructGraph(gpm_graph G){
		for(gpm_node n: G.vertexSet()){
			gpm_node ncopy = new gpm_node();
			ncopy.copyPN(n); //ncopy.exactcopyPN(n);
			this.InsertNode(ncopy);
		}
		for(gpm_edge e: G.edgeSet()){
			gpm_edge ei = new gpm_edge(e.from_node,e.to_node,e.e_bound,e.e_color);
			this.InsertEdge(ei);
		}
		this.gfilename = G.gfilename; //will be modified later.
		this.schema = G.schema;
		this.relschema = G.relschema;
		this.simMatchSet = new TreeMap<String, HashSet<String>>();	
		for(String s : G.simMatchSet.keySet()){
			HashSet<String> ms = new HashSet<String>(); 
			for(String ss : G.simMatchSet.get(s))
				ms.add(ss);
			this.simMatchSet.put(s, ms);
		}
	}
	
	public static void main(String[] args) throws IOException {
	gpm_graph gtest = new gpm_graph();
	int mode = 2;
	int time = 1;
	gtest.ConstructGraphFromNeo4j("/Users/qsong/Downloads/Simulation_test/test_query_1.db", mode);

	Hashtable<String,Hashtable<String,Integer>> hop1IndexPattern = new Hashtable<String,Hashtable<String,Integer>>();
	Hashtable<String,Hashtable<String,Integer>> hop2IndexPattern = new Hashtable<String,Hashtable<String,Integer>>();
	HashSet<gpm_node> hop1NodePattern = new HashSet<gpm_node>();
	
	for(gpm_node n : gtest.vertexSet()){
		hop1NodePattern.clear();
		if(!hop1IndexPattern.containsKey(n.tag)){
			Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
			hop1IndexPattern.put(n.tag, ll);
		}
		if(!hop2IndexPattern.containsKey(n.tag)){
			Hashtable<String,Integer> ll = new Hashtable<String,Integer>();
			hop2IndexPattern.put(n.tag, ll);
		}
		for(gpm_edge r : gtest.outgoingEdgesOf(n)){
			gpm_node n1 = gtest.GetVertex(r.to_node);
			hop1NodePattern.add(n1);
			if(!hop1IndexPattern.get(n.tag).containsKey(n1.nlabel)){
				hop1IndexPattern.get(n.tag).put(n1.nlabel, 1);
			}
			else{
				int newFrequency = hop1IndexPattern.get(n.tag).get(n1.nlabel)+1;
				hop1IndexPattern.get(n.tag).remove(n1.nlabel);
				hop1IndexPattern.get(n.tag).put(n1.nlabel, newFrequency);
			}
		}
		for(gpm_node hop1 : hop1NodePattern){
			for(gpm_edge r : gtest.outgoingEdgesOf(hop1)){
				gpm_node n2 = gtest.GetVertex(r.to_node);
				if(!hop2IndexPattern.get(n.tag).containsKey(n2.nlabel)){
					hop2IndexPattern.get(n.tag).put(n2.nlabel, 1);
				}
				else{
					int newFrequency = hop2IndexPattern.get(n.tag).get(n2.nlabel)+1;
					hop2IndexPattern.get(n.tag).remove(n2.nlabel);
					hop2IndexPattern.get(n.tag).put(n2.nlabel, newFrequency);
				}
			}
		}
	}
	for(String nodeid : hop2IndexPattern.keySet()){
		System.out.print(nodeid + "\t");
		for(String label : hop2IndexPattern.get(nodeid).keySet())
			System.out.println(label + "\t" + hop2IndexPattern.get(nodeid).get(label));
	}
	}
}

