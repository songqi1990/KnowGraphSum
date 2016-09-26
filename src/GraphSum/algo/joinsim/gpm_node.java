package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.util.HashMap;
import java.util.Vector;

public class gpm_node extends Object{

//	public HashMap<String, Vector<Object>> attrmap = null; //node attribute pairs
	public String tag=""; //node tag should be "0,1,2,...,k";(for mapping to distance matrix)
	public double weight;
	public String addinfo = "";
	public String nlabel;
	public int attidx = 0;

	//the following is for rankings for possible applications.
	public int rank = Integer.MIN_VALUE;
	
	//the following is for landmark vectors.
	//only used in landmark.
	public Vector<Short> distvf = null;
	public Vector<Short> distvt = null;

	public gpm_node(){
	}

	@SuppressWarnings("unchecked")
	public gpm_node(Vector<AVpair> alist, String tag, double weight, String label) {
		this.tag = tag;
		this.weight=weight;
		this.addinfo="";
		this.nlabel = label;
	}

	public gpm_node(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
	}
	
	//init a data node with a pattern node
	public void exactcopyPN(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
	}
	
	//init a data node with a pattern node
	public void copyPN(gpm_node a){
		this.tag = a.tag;
		this.weight = a.weight;
		this.addinfo = a.addinfo;
		this.nlabel = a.nlabel;
	}

	//return tag
	public String getTag(){
		return this.tag;
	}
}

