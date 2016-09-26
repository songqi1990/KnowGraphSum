package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;

import org.jgrapht.DirectedGraph;
import org.jgrapht.alg.StrongConnectivityInspector;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.tooling.GlobalGraphOperations;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.extension.KernelExtensionFactory;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

@SuppressWarnings("deprecation")
public class Neo4jGraph  {// EmbeddedGraphDatabase

	private static String DB_PATH = "/Users/qsong/Downloads/";
	public static GlobalGraphOperations ggdb;

	private GraphDatabaseService graphDb;

	private StrongConnectivityInspector<Node, Relationship> sccIns; 

	public String gfilename;
	//private static final String NAME_KEY = "name";
	
	public int gtype = 0; // pattern: 0; datagraph: 1
	
	public long distQtime = 0;
	public long distQtime2 = 0;
	public long distQtimeDM = 0;
	public long distQtime3 = 0;

	public Neo4j_distBuffer dBuff = null;

	public HashMap<String, String> schema; // schema on vertex. format: attr:type
	public Vector<String> relschema;//schema on edges: ie, colors

	public HashMap<Long, String> maxLabelofNode;

	public enum MyRelationshipTypes implements RelationshipType
	{
		CONTAINED_IN, KNOWS, PATH_EXTEND
	}
	
	public int[] rangeL = new int[3];
	public int[] rangeU = new int[3];
	
	public Set<Node> nodes;
	public Set<Relationship> edges;
	//~ CONSTRUCTOR --------------------------------------------------------------
	
	//graphType: 0 for pattern graph and 1 for data graph
	public Neo4jGraph(String path, int graphType) throws IOException {
		Vector<String> vec = new Vector<String>();
		
		gtype = graphType;
		DB_PATH = path;
		gfilename = path;
		try {
			this.graphDb = setUp(path);
			ggdb = GlobalGraphOperations.at(this.graphDb);
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("IO EXCEPTION");
		}
		try ( Transaction tx = this.getGDB().beginTx() ){
		nodes = IterToSet(ggdb.getAllNodes());
		vec.addElement("0");
		if(this.relschema==null)
			this.relschema = new Vector<String>();
		this.relschema.clear();
		this.relschema.addAll(vec);
		maxLabelofNode = new HashMap<Long, String>();
		edges = IterToSet(ggdb.getAllRelationships());
		if(graphType == 0){
			rangeL[0] = 1;
			rangeL[1] = 1;
			rangeL[2] = 1;
			rangeU[0] = 3;
			rangeU[1] = 4;
			rangeU[2] = 4;
		}
		tx.success();
		}
		//graphDb.shutdown();
	}


	
	//~ NEO4J Methods ------------------------------------------------------------

	public GraphDatabaseService setUp(String path) throws IOException
	{
		GraphDatabaseService a = 
				new GraphDatabaseFactory().newEmbeddedDatabase( path );
		registerShutdownHook(a);
		return a;
	}

	public void shutdown()
	{
		this.shutdown();
	}

	private static void registerShutdownHook(final GraphDatabaseService graphDb)
	{
		// Registers a shutdown hook for the Neo4j instance so that it
		// shuts down nicely when the VM exits (even if you "Ctrl-C" the
		// running application).
		Runtime.getRuntime().addShutdownHook( new Thread()
		{
			@Override
			public void run()
			{
				graphDb.shutdown();
			}
		} );
	}

	//~ neo4jGraph Helpers ----------------------------------------------------

	public GlobalGraphOperations getGGO () {
		return ggdb;
	}
	
	public GraphDatabaseService getGDB() {
		return graphDb;
	}

	public Set<Node> getIncomingNodes(Node n) {
		Set<Node> nodes = new HashSet<Node>();
		for ( Relationship r : n.getRelationships(Direction.INCOMING) ) 
		{
			Node other = r.getOtherNode(n);
			nodes.add(other);
		}
		return nodes;

	}

	public int getEdgeNumber() {
		Iterable<Relationship> iter = ggdb.getAllRelationships();
		int cnt = 0;
		for (Relationship r : iter ) {
			cnt++;
		}
		return cnt;
	}

	public Vector<Node> getIncomingNodes(Node n, String param) {
		Vector<Node> nodes = new Vector<Node>();
		for ( Relationship r : n.getRelationships(Direction.INCOMING, 
				DynamicRelationshipType.withName(param)) ) 
		{
			Node other = r.getOtherNode(n);
			nodes.add(other);
		}
		return nodes;

	}

	public static <T> Set<T> IterToSet(Iterable<T> iter) {
		Set<T> set = new HashSet<T>();
		for ( T r : iter) {
			set.add(r);
		}
		return set;
	}

	public <T> Vector<T> IterToVector(Iterable<T> iter) {
		Vector<T> vec = new Vector<T>();
		for ( T r : iter) {
			vec.add(r);
		}
		return vec;

	}
	
	public Set<Node> getAllNodes() {
		
		return nodes;
	}
	
	public Set<Relationship> getAllEdges() {
		return IterToSet(ggdb.getAllRelationships());
	}
	
	public Set<Relationship> getAllEdges(Node a, Node b) {
		Set<Relationship> rels = new HashSet<Relationship>();
		for (Relationship r : a.getRelationships(Direction.BOTH))
		{
			if (r.getEndNode() == b || r.getStartNode() == b)
				rels.add(r);
		}
		return rels;
	}
	
	public Relationship getEdge(Node a, Node b) {
		for (Relationship r : a.getRelationships(Direction.OUTGOING)){
			if(r.getEndNode() == b)
				return r;
		}
		return null;
	}
	
	

	
	public Set<Relationship> outgoingEdgesOf(Node n) {
		
		return IterToSet(n.getRelationships(Direction.OUTGOING));
		
	}
	
	public Set<Node> outgoingNodesOf(Node n) {
		Set<Node> nodes = new HashSet<Node>();
		
		for (Relationship r : n.getRelationships(Direction.OUTGOING)) {
			nodes.add(r.getOtherNode(n));
		}
		return nodes;
	}

	public int getNodeNumber() {
		Iterable<Node> nodes = ggdb.getAllNodes();
		int cnt = 0;
		for (Node n : nodes ) {
			cnt++;
		}
		return cnt;

	}
	
	public Node reproduceNode(Node a) {
		Node n = graphDb.createNode();
		for (Label l : a.getLabels()) {
			n.addLabel(l);
		}
		for (String s : a.getPropertyKeys()) {
			n.setProperty(s, a.getProperty(s));
		}
		n.setProperty("tag", a.getId());
		return n;
	}
	
	// idk about this
	public Relationship reproduceEdge(Relationship r) {
		Node a = r.getStartNode(), b = r.getEndNode();
		
		Node newA = this.reproduceNode(a);
		Node newB = this.reproduceNode(b);
		
		Relationship rel = newA.createRelationshipTo(newB, r.getType());
		rel.setProperty("tag", r.getId());
		for (String s : r.getPropertyKeys()) {
			rel.setProperty(s, r.getProperty(s));
		}
		return rel;
	}
	
	
	public StrongConnectivityInspector<Node, Relationship> getSCC() {
		return sccIns;
	}
	
	public void setSCC(StrongConnectivityInspector<Node, Relationship> scc) {
		sccIns = scc;;
	}

	//~ methods from Neo4jGraph (reinvented) -----------------------------------
	public Node GetVertex(Long ntag){
		Node no = graphDb.getNodeById(ntag);
		if (no != null)
			return no;
		else
			return null;
	}

	public Vector<Node> GetChildren(Node n, int color){
		Vector<Node> cset = new Vector<Node>();
		for(Relationship e : n.getRelationships(Direction.OUTGOING)){
			if(color!=-1 && e.hasProperty("color")) {
				cset.add(e.getEndNode());
			}
			else if(color==-1)
				cset.add(e.getEndNode());
		}
		return cset;
	}

	public Relationship getRelationship(long fid, long tid){
		Node fn  = graphDb.getNodeById(fid);
		Node tn = graphDb.getNodeById(tid);
		return(getRelationship(fn,tn));
	}

	public Relationship getRelationship(Node fn, Node tn){
		for (Relationship r : fn.getRelationships(Direction.OUTGOING)) {
			if(r.getEndNode().getId() == tn.getId()){
				return r;
			}
		}
		return null;
	}

	public boolean containsEdge(Node getVertex, Node getVertex2) {
		Relationship r = this.getRelationship(getVertex, getVertex2);
		if (r == null) {
			return false;
		} else {
			return true;
		}

	}

	public boolean extendEdge(Vector<Node> nlist, Node fn, Node tn){
		RelationshipType rt = MyRelationshipTypes.PATH_EXTEND;

		if(this.getRelationship(fn, tn)==null)
			return false;
		Relationship e = this.removeEdge(fn, tn);
		fn.createRelationshipTo(nlist.elementAt(0), rt);

		for(int i = 1; i < nlist.size() - 1; i++) {
			nlist.elementAt(i).createRelationshipTo(nlist.elementAt(i+1), rt);
		}

		nlist.elementAt(nlist.size()-1).createRelationshipTo(tn, rt);	

		return true;
	}

	// got rid of color functionality
	public boolean removeEdge(Node a, Node b, int color){
		Relationship e = this.getRelationship(a.getId(), b.getId());

		if(e==null)
			return false;
		else {
			e.delete();
			return true;
		}
	}

	// got rid of color functionality
	public Relationship removeEdge(Node a, Node b) {
		Relationship e = this.getRelationship(a.getId(), b.getId());

		if(e==null)
			return null;
		else {
			e.delete();
			return e;
		}
	}

	public Relationship removeEdge(Relationship e ) {

		if(e==null)
			return null;
		else {
			e.delete();
			return e;
		}
	}

	// need more "random" code
	public Relationship removeRandomEdge(Node mnode){ 
		Relationship rel = null;
		for (Relationship r : ggdb.getAllRelationships() ) {
			rel = r;
			break;
		}
		return rel;	
	}

	public void ClearNodeWeight(){
		for (Node n : ggdb.getAllNodes()) {
			if (n.hasProperty("weight")) {
				n.removeProperty("weight");
			}
		}
	}

	public void ClearNodeInfo(){
		for (Node n : ggdb.getAllNodes()) {
			if (n.hasProperty("info")) {
				n.removeProperty("info");
			}
		}
	}

	public boolean checkDistanceWithBound(Node a, Node b, 
			int bound){
		if(a.getId() == (b.getId())){
			if(this.getRelationship(a, b)==null)
				return false;
			else
				return true;
		}
		PathFinder<Path> finder = GraphAlgoFactory.shortestPath(
                PathExpanders.forDirection(Direction.BOTH ), 100 );
        Path foundPath = finder.findSinglePath( a, b );
        if(foundPath == null)
        	return false;
        int dist = foundPath.length();
		if(dist<=bound)
			return true;
		else
			return false;
	}
	
	public boolean checkDistance(Node a, Node b){
		Iterable<Relationship> arel = a.getRelationships(Direction.OUTGOING);
		for (Relationship item : arel)
		{
			if (item.getEndNode().equals(b))
				return true;
		}
		return false;
	}
	
	

	public short getDist(Node a, Node b, int mode, int color) {
		// TODO Auto-generated method stub

		if( a.getId() == b.getId() )  {
			return 0;
		}

		if(mode==1){
			//	return FS.shortestDistance(a, b, color);
		}
		else{
			/*			if(dBuff.getDist(a.getId(), b.getId(), color)==-1){
					//System.out.println("recompute");
					boolean reachable = checkDistanceFromDS(mode, false, a, b, 
						this.getNodeNumber(), color, false);
					if(!reachable)
						return Short.MAX_VALUE;
					else
						return ((Integer)dBuff.getDist(a.getId(), 
							b.getId(), color)).shortValue();
				}
				else
					return ((Integer)dBuff.getDist(a.getId(), b.getId(), color)).shortValue();
			 */			}
		return -1;
	}


	//	This function checks the distance of two vertex with ad-hoc BFS search
	//	utilizing bounded DijkstraShortestPath
	// 	when start and end vertices are specified -- becomes BFS search.
	//	============================================================================//
	public boolean checkDistanceFromDS(int mode, boolean opt, Node a, Node b, 
			int bound, int color, boolean inc){

		long bibfs = 0;
		long bfs = 0;

		if(a.getId() == b.getId()){
			if(this.getRelationship(a, b)!=null){
				return true;
			}
			else
				return false;
		}
		/*
		//distance stored in dist buffer.
		if(this.dBuff!=null){
			int dist = this.dBuff.getDist(a.getId(), b.getId(), color);
			int status = 1;
			if(inc)
				status = this.dBuff.getstatus(a.getId(), b.getId(), color);
			if(dist!=-1 && status==1){
				if(dist<=bound)
					return true;
				return false;
			}
		}

		if(this.dBuff==null){
			this.dBuff = new gpm_distBuffer(gfilename);
		}

		 */

		long start1 = System.nanoTime();

		HashSet<Node> neighbors1 = new HashSet<Node>();
		HashSet<Node> neighbors2 = new HashSet<Node>();
		HashSet<Node> S3 = new HashSet<Node>();

		Vector<Node> visited1 = new Vector<Node>();
		Vector<Node> visited2 = new Vector<Node>();
		Vector<Node> newnodes1 = new Vector<Node>();
		Vector<Node> newnodes2 = new Vector<Node>();
		neighbors1.add(a);
		neighbors2.add(b);
		visited1.add(a);
		visited2.add(b);
		newnodes1.add(a);
		newnodes2.add(b);

		short distn1 = 0;
		short distn2 = 0;
		boolean s1change = true;
		boolean s2change = true;
		boolean isr = false;
		//		HashSet<Node> cset = new HashSet<Node>();

		short i = 0;
		for(i=0;i<Math.min(bound, this.getNodeNumber()); i++){

			//if s1 smaller than s2 extend s1
			if(visited1.size()<=visited2.size() && s1change ){
				s1change = true;
				neighbors1.clear();
				for(Node as: newnodes1){
					neighbors1.addAll(this.GetChildren(as, color));
				}
				neighbors1.removeAll(visited1);
				if(neighbors1.size()==0){
					s1change = false;
				}
				distn1++;
				if(opt){
					for(Node as:neighbors1){	
						dBuff.insertDist(0, a.getId(), as.getId(), distn1, color, inc);
					}
				}
				visited1.addAll(neighbors1);
				newnodes1.clear();
				newnodes1.addAll(neighbors1);
			}

			//if s1 larger than s2 extend s2.
			else if(visited2.size()<visited1.size() && s1change && s2change){
				s2change = true;
				neighbors2.clear();
				for(Node bs: newnodes2){
					neighbors2.addAll(this.GetParents(bs, false, color));
				}
				neighbors2.removeAll(visited2);
				if(neighbors2.size()==0){
					s2change = false;
				}
				distn2++;
				if(opt){
					for(Node bs:neighbors2){
						dBuff.insertDist(0, bs.getId(), b.getId(), distn2, color, inc);
					}
				}
				visited2.addAll(neighbors2);
				newnodes2.clear();
				newnodes2.addAll(neighbors2);
			}

			S3.clear();
			S3.addAll(visited1);
			S3.retainAll(visited2);
			if(S3.size()>0){
				dBuff.insertDist(0, a.getId(), b.getId(), (short)(i+1), color, inc);
				isr =  true;
				break;
			}
			else if((!s1change)|| (s1change && !s2change) ){ //&& !s2change //
				isr =  false;
				break;
			}
		}

		bibfs = System.nanoTime() - start1;
		distQtime += bibfs;

		start1 = System.nanoTime();

		if(i>=this.getNodeNumber()-1){
			dBuff.insertDist(0, a.getId(), b.getId(), Short.MAX_VALUE, color, inc);
		}


		//using BFS search
		return isr;
	}
	//	}


	//	=====================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//  if color = -1, all colored edges will be added.
	//	=====================================================================//
	public Vector<Node> GetParents(Node n, boolean sort, int color){
		Vector<Node> pset = new Vector<Node>();
		Vector<Integer> bound = new Vector<Integer>();

		pset = this.getIncomingNodes(n, String.valueOf(color));

		/*
			if(sort){
				bound.add(e.e_bound);
				for(int i = bound.size()-1; i>0;i--){
					if(bound.elementAt(i).compareTo(bound.elementAt(i-1))>0){
						Integer tmp = bound.elementAt(i-1);
						bound.set(i-1, bound.elementAt(i));
						bound.set(i, tmp);
						Node tmpn = pset.elementAt(i-1);
						pset.set(i-1, pset.elementAt(i));
						pset.set(i, tmpn);
					}
				}
			}
		 */		
		return pset;
	}


	//	============================================================================//
	//	This function expands an scc node into a node set
	//	============================================================================//
	public HashSet<Node> expSCC(Node sccnode){
		String s = sccnode.getProperty("tag").toString();
		Vector<Long> nids = createVectorFromString(s,"_");
		HashSet<Node> nset = new HashSet<Node>();
		for(Long id: nids){
			nset.add(this.GetVertex(id));
		}
		return nset;
	}

	public Vector<Long> createVectorFromString(String strContent,
			String strDelimiter) {
		Vector<Long> vec = new Vector<Long>();
		String[] words = strContent.split(strDelimiter);

		for (int i = 0; i < words.length; i++) {
			vec.addElement(Long.parseLong(words[i]));
		}
		return vec;
	}

	
	
	public static void main(String[] args) throws IOException {

		Vector<String> vec = new Vector<String>();

		for (int i = 0; i < 4; i++) {
			String a = "node" + i;
			vec.add(a);
		}

		for (String s : vec) {
			System.out.println(s);
		}
		String start = "Start";
		String end = "end";

		Vector<String> path = new Vector<String>();

		path.add(start + "-->" + vec.elementAt(0));

		for(int i = 1; i < vec.size() - 1; i++) {
			path.add(vec.elementAt(i) + "-->" + vec.elementAt(i+1));
		}

		path.add(vec.elementAt(vec.size() - 1) + "-->" + end);

		for (String s : path) {
			System.out.println(s);
		}
	}




	/*
		public String gfilename = "";

		public int gtype = 0; // pattern: 0; dagetId()raph: 1

		public HashMap<String, String> schema; // schema on vertex. format: attr:type
		//	public Vector<Comparator> complist; // comparator of schema -- for index.

		public Vector<String> relschema;//schema on edges: ie, colors

		//distance matrix.
		//	public gpm_distMatrix M = null;
		public gpm_distFWMatrix FS = null;

		//distance index buffer.
		//increasing at runtime.
		//may introduce a new issue: buffer management and replacement algorithm.
		//based on a cost model.
		public gpm_distBuffer dBuff = null;
		public int distbuffsize = 0;

		public StrongConnectivityInspector<Node, Relationship> sccIns; 

		public long distQtime = 0;
		public long distQtime2 = 0;
		public long distQtimeDM = 0;
		public long distQtime3 = 0;

		public int libattsize = 3;// data nodes contains 3 types of nodes


		private static final long serialVersionUID = 1L;
	 */

	//	============================================================================//
	//	This function returns attribute type
	//	============================================================================//
	public String attrtype(String attname){
		return this.schema.get(attname);
	}

	//	============================================================================//
	//	This function casts Neo4jGraph into directedgraph class.  
	//	============================================================================//
	@SuppressWarnings("unchecked")
	public Neo4jGraph Cast2DG(){ 
		/*	Neo4jGraph DG = 
				new Neo4jGraph("/home/rbaumher/REU");
				for(Node n: this.ggdb.getAllNodes())
					DG.addVertex(n);
				for(Relationship e: ggdb.getAllRelationships()) {
					DG.addEdge(this.GetVertex(e.getStartNode()),this.GetVertex((e.getEndNode())),e);
				}
				return DG;
		 */
		// this seems unneccissary...
		return this;
	}

	// this doesn't make sense
	//	============================================================================//
	//	This function inserts a user-defined node into graph 
	//	============================================================================//
	public boolean InsertNode(Node n){
		//n.getId() = ""+(this.getNodeNumber()+1);
		//return(this.addVertex(n));
		
		return false;
	}

	//	============================================================================//
	//	This function copy a node num times. 
	//	============================================================================//
	public void copyNode(Node a, int num, int maxenum){
		int nodenum = this.getNodeNumber();
		this.GetParents(a, false,-1);
		Vector<Node> cset = this.GetChildren(a,-1);
		for(int i=0;i<num;i++){
			Node ai = reproduceNode(a);
			ai.setProperty("tag", nodenum+i);
			
			int ecolor = 0;
			for(Node cn:cset){
				if(this.getRelationship(a.getId(), cn.getId())==null){
					System.out.println("here.");
					break;
				}
				if ( this.getRelationship(a.getId(), cn.getId()).hasProperty("color") ) {
					ecolor = (int) this.getRelationship(a.getId(), cn.getId()).getProperty("color");
				}
				int bound = -1;
				if ( this.getRelationship(a.getId(), cn.getId()).hasProperty("bound") ) {
					ecolor = (int) this.getRelationship(a.getId(), cn.getId()).getProperty("bound");
				}
				this.InsertEdge(ai, cn, bound, ecolor);
			}
		}
	}


	//	============================================================================//
	//	This function copy a pattern node num times. 
	//	============================================================================//
	public void copyPNode(Node a, int num){
		int nodenum = this.getNodeNumber();
		this.GetParents(a, false,-1);
		Vector<Node> cset = this.GetChildren(a,-1);
		for(int i=0;i<num;i++){
			
			Node ai = reproduceNode(a);
			ai.setProperty("tag", (int)(nodenum+i));
			
			int ecolor = -1;
			//			for(Node pn:pset){
			//				if(this.getRelationship(pn.getId(), a.getId())==null){
			//					System.out.println("here.");
			//				}
			//				ecolor = this.getRelationship(pn.getId(), a.getId()).e_color;
			//				this.InsertEdge(pn, ai, this.getRelationship(pn.getId(), a.getId()).e_bound, ecolor);
			//			}
			for(Node cn:cset){
				if(this.getRelationship(a.getId(), cn.getId())==null){
					System.out.println("here.");
				}
				if ( this.getRelationship(a.getId(), cn.getId()).hasProperty("color") ) {
					ecolor = (int) this.getRelationship(a.getId(), cn.getId()).getProperty("color");
				}
				int bound = -1;
				if ( this.getRelationship(a.getId(), cn.getId()).hasProperty("bound") ) {
					bound = (int) this.getRelationship(a.getId(), cn.getId()).getProperty("color");
				}
				
				this.InsertEdge(ai, cn, bound, ecolor);
			}
		}
	}

	//	============================================================================//
	//	This function inserts a user-defined edge into graph 
	//	============================================================================//
	public boolean InsertEdge(Node a, Node b, int ebound, int color){
		if (a == null || b == null) {
			return false;
		}
		RelationshipType rt = MyRelationshipTypes.KNOWS;
		Relationship e = a.createRelationshipTo(b, rt);

		e.setProperty("color", color);

		return true;
	}

	//	============================================================================//
	//	This function inserts an edge according to node getId() 
	//	============================================================================//
	//	public boolean InsertEdge(String a, String b, double ebound){
	//		Relationship e = new Relationship(a, b, ebound);
	//		Node na = this.GetVertex(a);
	//		Node nb = this.GetVertex(b);
	//		return(this.addEdge(na, nb, e));
	//	}

	//	============================================================================//
	//	This function inserts a user-defined edge into graph  
	//	============================================================================//
	public boolean InsertEdge(Relationship e){
		if(e!=null){
			Node a = e.getStartNode();
			Node b = e.getEndNode();
		
			return true;
		}
		return false;
	}

	//	============================================================================//
	//	This function inserts an random edge into graph  
	//	============================================================================//
	public Relationship InsertRandomEdge(Node cn){

		double rate = 0.5;
		Relationship e = null;
		if(Math.random()<rate){
			Node b = this.getAnode();
			while(this.getRelationship(cn, b)!=null)
				b = this.getAnode();
			cn.createRelationshipTo(b, MyRelationshipTypes.KNOWS);  // what is 1, 0 param???
		//	e = new Relationship(cn.getId(), b.getId(), 1,0);
		}
		else{
			Node a = this.getAnode();
			Node b = this.getAnode();
			while(this.getRelationship(a, b)!=null){
				a = this.getAnode();
				b = this.getAnode();
			}
			a.createRelationshipTo(b, MyRelationshipTypes.KNOWS);
			//e = new Relationship(a.getId(),b.getId(),1,0);
		}
		return e;
	}



	//	============================================================================//
	//	This function removes all self-loops
	//	============================================================================//
	public void clearSLoops(){
		Vector<Relationship> eset = new Vector<Relationship>();

		eset = IterToVector(ggdb.getAllRelationships());

		for(int i=0;i<eset.size();i++){
			if(eset.elementAt(i).getStartNode().equals(eset.elementAt(i).getEndNode())){
				this.removeEdge(eset.elementAt(i));
			}
		}
	}





	//	============================================================================//
	//	This function gets a random node
	//	============================================================================//
	public Node getAnode(){
		Vector<Node> vlist = new Vector<Node>();

		vlist = IterToVector(ggdb.getAllNodes());
		return vlist.elementAt((int)(Math.random()*vlist.size()));
	}

	//	============================================================================//
	//	This function gets a random edge
	//	============================================================================//
	public Relationship getAnedge(){
		Vector<Relationship> elist = new Vector<Relationship>();

		elist = IterToVector(ggdb.getAllRelationships());
		return elist.elementAt((int)(Math.random()*elist.size()));
	}



	//	============================================================================//
	//	This function gets incoming edges (sorted) of a node
	//	============================================================================//
	public Vector<Relationship> GetParentEdge(Node n, boolean sort, int color){
		Vector<Node> pnvec = this.GetParents(n, sort, color);
		Vector<Relationship> envec = new Vector<Relationship>();
		for(Node pn: pnvec){
			Relationship e = this.getRelationship(pn, n);
			envec.add(e);
		}
		return envec;
	}

	//	============================================================================//
	//	This function gets outcoming edges (sorted) of a node
	//	============================================================================//
	public Vector<Relationship> GetChildEdge(Node n, int color){
		Vector<Node> cnvec = this.GetChildren(n, color);
		Vector<Relationship> envec = new Vector<Relationship>();
		for(Node cn: cnvec){
			Relationship e = this.getRelationship(cn, n);
			if(e!=null)
				envec.add(e);
		}
		return envec;
	}

	//	============================================================================//
	//	This function gets outcoming edges (sorted) of a node set
	//	============================================================================//
	public HashSet<Relationship> GetChildEdgeSet(HashSet<Node> nset){
		HashSet<Relationship> eset = new HashSet<Relationship>();
		for(Node n:nset)
			eset.addAll(this.outgoingEdgesOf(n));
		return eset;
	}


	//	============================================================================//
	//	This function gets parent set of a node 
	//	if sort, the parents will be sorted according to edge weights
	//	============================================================================//
	public HashSet<Node> GetParentsSet(HashSet<Node> nset,  int color){
		HashSet<Node> psets = new HashSet<Node>();
		//		HashSet<Node> center = new HashSet<Node>();
		//		center.addAll(nset);
		for(Node n: nset){
			psets.addAll(GetParents(n,false,color));
		}
		return psets;
	}

	//	============================================================================//
	//	This function gets child set of a node set
	//	============================================================================//
	public HashSet<Node> GetChildrenSet(HashSet<Node> nset, int color){
		HashSet<Node> cset = new HashSet<Node>();
		for(Node n: nset){
			cset.addAll(GetChildren(n,color));
		}
		return cset;
		//		for(Relationship e:this.outgoingEdgesOf(n)){
		//			if(color!=-1 && ((Relationship)e).e_color==color){
		//				cset.add((Node)this.getRelationshipTarget(e));
		//			}
		//			else if(color==-1)
		//				cset.add((Node)this.getRelationshipTarget(e));
		//		}
		//		return cset;
	}


	//	============================================================================//
	//	This function gets all the reachable nodes from a set of node
	//	============================================================================//
	public HashSet<Node> GetReachableNSet(HashSet<Node> nset){
		HashSet<Node> rset = new HashSet<Node>();
		//rset.addAll(nset);
		HashSet<Node> newneighbors = new HashSet<Node>();
		newneighbors.addAll(nset);
		HashSet<Node> tmpPset = new HashSet<Node>();
		tmpPset.addAll(nset);

		while(newneighbors.size()>0){
			rset.addAll(newneighbors);
			newneighbors.clear();
			newneighbors.addAll(this.GetChildrenSet(tmpPset, -1));
			newneighbors.removeAll(tmpPset);
			tmpPset.clear();
			tmpPset.addAll(newneighbors);
		}
		return rset;
	}

/**
 * doesnt save properties right now
 */
	//	============================================================================//
	//	This function reverses a graph
	//	============================================================================//
	public void Reverse(){
		for(Relationship e: ggdb.getAllRelationships()){
		
			Node a = e.getStartNode();
			Node b = e.getEndNode();
			RelationshipType rt = e.getType();
		
			this.removeEdge(e);
			
			b.createRelationshipTo(a, rt);
		}
	}

	@SuppressWarnings("unchecked")
	public Neo4jSubGraph sccDAG(){
		Neo4jGraph dg = this; 
		Neo4jSubGraph sccDAG = new Neo4jSubGraph();
		if(sccIns==null)
			sccIns = new StrongConnectivityInspector((DirectedGraph) dg);
		List<Set<Node>> sccsets = sccIns.stronglyConnectedSets();
		//List<Set<Node>> sccsets = dg.nodes;
		
		Set<Node> WF = new HashSet<Node>();
		HashSet<Node> NWF = new HashSet<Node>();

		Vector<Node> sccnodes = new Vector<Node>();
		HashSet<Relationship> sccedges = new HashSet<Relationship>();
		//int sccid = 0;

		for(int i=0;i<sccsets.size();i++){

			Set<Node> s = sccsets.get(i);

			for(Node n: s){
				n.setProperty("info", ""+i);
				sccnodes.addElement(n);
				
			}

//			Node scc = sccnodes.createNode();
//			System.out.println("SCC:" + (i));
//			//scc.getId() = ""+sccid;
//			for(Node n: s){
//				scc.setProperty("info", scc.getProperty("info").toString() + n.getId() + "_");
//			}
//			scc.getId() = scc.getProperty("info").substring(0,scc.getProperty("info").lastIndexOf("_"));
//			sccnodes.add(scc);

//			sccedges.add(new Relationship(sccnodes.elementAt(i).getId(), sccnodes.elementAt(i).getId(),1,0));

			if(sccsets.get(i).size()>1)
				NWF.addAll(sccsets.get(i));
		}

		//propagate nonWF area
		//int NWFsize = 0;
		HashSet<Node> newnf = this.GetParentsSet(NWF, -1);
		HashSet<Node> tmp = new HashSet<Node>();
		tmp.addAll(newnf);

		while(newnf.size()> 0){
			NWF.addAll(newnf);
			newnf.clear();
			newnf.addAll(this.GetParentsSet(tmp, -1));
			newnf.removeAll(NWF);
			tmp.clear();
			tmp.addAll(newnf);
		}

		WF = IterToSet(ggdb.getAllNodes());
		WF.removeAll(NWF);

		//int i=0;
		//computing ranks
		for(int i=0;i<sccsets.size();i++){
			Set<Node> s = sccsets.get(i);
			//sccid++;

			for(int j=0;j<i && j!=i;j++){
				Set<Node> s2 = sccsets.get(j);
				for(Node na: s){
					for(Node nb: s2){
						if(this.getRelationship(na, nb)!=null){
							Relationship e = this.getRelationship(na, nb);
									//new Relationship(sccnodes.elementAt(i).getId(),sccnodes.elementAt(j).getId(),1,0);
							sccedges.add(e);
							break;
						}
						else if(this.getRelationship(nb, na)!=null){
							Relationship e = this.getRelationship(nb, na);
									//new Relationship(sccnodes.elementAt(j).getId(),sccnodes.elementAt(i).getId(),1,0);
							sccedges.add(e);
							break;
						}
					}
					break;
				}
			}
		}


		System.out.println("Original graph nodes:" + this.getNodeNumber() + "edges: "+ this.getEdgeNumber());

		System.out.println("SCC nodes and edges done.");
		System.out.println("nodes--" + sccnodes.size() + "edges--" + sccedges.size());

		String sccname = gfilename.substring(0, gfilename.indexOf("."));
		sccname = sccname + "_sccDAG.grp";
	//	sccDAG.ConstructGraphFromVec(0, sccname, this.schema, sccnodes, sccedges, this.relschema);

		return sccDAG;
	}

	
	//	============================================================================//
	//	This function returns node induced subgraph given a set of nodes
	//	============================================================================//
	public Neo4jSubGraph nsubGraph(HashSet<Node> nset){
		
			Neo4jSubGraph nsub = new Neo4jSubGraph();

			for(Node n: nset){
				nsub.addVertex(n);
			}
			
			for(Node a: nset){
				for(Node b:nset){
					if(this.getRelationship(a, b)!=null){
						nsub.addEdge(this.getRelationship(a, b));
					}
				}
			}
			
			nsub.setSuperGraph(this.gfilename);
			return nsub;
		
	}

	//	============================================================================//
	//	This function returns edge induced subgraph given a set of edges
	//	============================================================================//
	public Neo4jSubGraph esubGraph(HashSet<Relationship> eset){
		Neo4jSubGraph esub = new Neo4jSubGraph();
		
		for(Relationship e: eset){
			esub.addEdgeWithNodes(e);
		}
		return esub;
	}


	//	============================================================================//
	//	This function returns a subgraph induced by all the reachable nodes from a node set
	//	See "Fast bisimulation algorithm"
	//	============================================================================//
	public Neo4jSubGraph rsubGraph(HashSet<Node> nset){
	
		HashSet<Node> rnset = this.GetReachableNSet(nset);
		HashSet<Relationship> reset = this.GetChildEdgeSet(rnset);
		
		Neo4jSubGraph esub = new Neo4jSubGraph(rnset, reset);
		
		return esub;
		
	}
	
	//	============================================================================//
	//	This function checks if a node contains a relationship
	//	============================================================================//
	public boolean CheckEdgePropertyContain(String id, String rel){
		Node n = this.graphDb.getNodeById(Long.valueOf(id));
		for(Relationship r : n.getRelationships()){
			for (String key : r.getPropertyKeys()){
				if(r.getProperty(key).equals(rel)){
					return true;
				}
			}
		}
		return false;
	}
	
	public HashSet<String> GetNewNodesByRelationship(String id, String rel){
		HashSet<String> res = new HashSet<String>();
		Node n = this.graphDb.getNodeById(Long.valueOf(id));
		for(Relationship r : n.getRelationships(Direction.OUTGOING)){
			for(String key : r.getPropertyKeys()){
				if(r.getProperty(key).equals(rel))
					res.add(String.valueOf(r.getEndNode().getId()));
			}
		}
		return res;
	}
}
