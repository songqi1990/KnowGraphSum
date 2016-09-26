package wsu.eecs.mlkd.KGQuery.algo.SPMine;

import java.util.HashSet;
import java.util.Set;

import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_graph;
import wsu.eecs.mlkd.KGQuery.algo.joinsim.gpm_node;

public class newdivSP_pair {
	public gpm_graph p1;
	public gpm_graph p2;
	int size;
	public double pairWeight=0.0;
	double pairInterestingness = 0.0;
	public MinHash minhash;
	
	public newdivSP_pair(){
		size = 0;
		minhash = new MinHash(0.01, 200);
	}
	public newdivSP_pair(gpm_graph _p1, gpm_graph _p2){
		p1 = new gpm_graph();
		p2 = new gpm_graph();
		p1=_p1;
		p2=_p2;
		size = 2;
		minhash = new MinHash(0.01, 200);
	}
	
	public void PairInterestingnessCalculate(){
		CheckHashResult(this.p1);
		CheckHashResult(this.p2);
		this.pairInterestingness = p1.interestingness + p2.interestingness + Distance(p1, p2);
	}
	
	//insert a pattern into the pair
	//flag:1 insert first pattern
	//flag:2 insert second pattern
	public void InsertPattern(gpm_graph g, int flag){
		if(flag == 1){
			this.p1 = g;
			this.size ++;
		}
		if(flag == 2){
			this.p2 = g;
			this.size ++;
		}
	}
	
	//this function calculate distance of two graphs
	public double Distance(gpm_graph g1, gpm_graph g2){
		return (minhash.similarity(g1.hashResult, g2.hashResult));
	}
		
	//this function checks if a pattern already have a hash result
	public void CheckHashResult(gpm_graph g){
		Set<Long> set = new HashSet<Long>();
		if(g.hashResult[0] != 0)
			return;
		else{
			for(gpm_node n : g.vertexSet())
				set.add(Long.valueOf(n.tag));
			g.hashResult = minhash.signature(set);
		}
	}
	
	//clear 
	void clear(){
		this.size = 0;
		this.pairInterestingness = 0.0;
	}
}
