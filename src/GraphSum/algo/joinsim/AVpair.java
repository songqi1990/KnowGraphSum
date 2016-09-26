package wsu.eecs.mlkd.KGQuery.algo.joinsim;

import java.util.Vector;

// attribute-op-value 
//for pattern node: predicts. 
//for data node: attribute-value pair, op = "=".
public class AVpair {
	
	public String Att = "";
	@SuppressWarnings("unchecked")
	public Vector opval = null;
	
	@SuppressWarnings("unchecked")
	public AVpair(String att, String op, Object val){
		if(opval==null)
			opval = new Vector<Object>();
		Att = att;
		opval.add(op);
		opval.add(val);
	}
	
	@Override
	public String toString() {
		return Att+opval.toString();
	}

}
