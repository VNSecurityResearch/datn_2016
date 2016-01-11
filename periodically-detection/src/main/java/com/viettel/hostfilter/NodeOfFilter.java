/**
 * 
 */
package com.viettel.hostfilter;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;


/**
 * Present a node of tree domain.
 * @author THANHNT78
 *
 */
public class NodeOfFilter implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String name;
	public Map<String, NodeOfFilter> children;
	/**
	 * Create a node 
	 * @param name domain ia name of node
	 * 
	 * */
	public NodeOfFilter(String name){
		this.name = name;
		children = new HashMap<String, NodeOfFilter>();
	}
	/**
	 * get subname of domain name 
	 * @param subname subname of domain to find
	 * @return node contain subname. 
	 *  	Return null if subname is not exist.
	 * */
	public NodeOfFilter getChildren(String subname){
		if (children != null) {
			if (children.containsKey(subname)) {
				return children.get(subname);
			}
		}
		return null;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
