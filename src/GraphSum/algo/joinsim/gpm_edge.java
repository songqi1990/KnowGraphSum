package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import org.jgraph.graph.DefaultEdge;

@SuppressWarnings("serial")
public class gpm_edge extends DefaultEdge{

	public String from_node = "";
	public String to_node = "";
	public int e_bound;
	public int e_color = 0;
	
	public gpm_edge(){
	}
	
	public gpm_edge(String fn, String tn, int eb, int color){
		from_node = fn;
		to_node = tn;
		e_bound = eb;
		e_color = color;
	}
	
	public boolean isconnect(String fn, String tn){
		if(from_node.equals(fn) && to_node.equals(tn))
			return true;
		return false;
	}
	
	@Override
	public String toString() {
		return this.from_node + "-" + this.to_node;
	}
}
