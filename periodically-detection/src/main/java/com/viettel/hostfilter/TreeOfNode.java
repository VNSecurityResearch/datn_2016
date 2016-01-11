/**
 * 
 */
package com.viettel.hostfilter;

import java.io.Serializable;

import org.apache.commons.lang.ArrayUtils;

/**
 * Present a tree domain
 * @author THANHNT78
 *
 */
public class TreeOfNode implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private NodeOfFilter root;
	/**
	 * Create a tree with root.
	 * */
	public TreeOfNode(){
		root  = new NodeOfFilter(" ");
	}
	/**
	 * Insert domain in tree
	 * @param name_in domain
	 * */
	public void insert(String name_in){
		String[] name = name_in.split("\\.");
		ArrayUtils.reverse(name);
		
		NodeOfFilter current =root;
		for (String string : name) {
			if(current.getChildren(string)==null)
				current.children.put(string, new NodeOfFilter(string));
			current = current.getChildren(string);
		}
	}
	/**
	 * Find domain on tree
	 * @param name_in domain
	 * @return true if domain is in tree. false if domain isn't in tree.
	 * */
	public boolean find(String name_in){
		String[] name = name_in.split("\\.");
		ArrayUtils.reverse(name);
		
		NodeOfFilter current = root;
		for (String string : name) {
			if(current.getChildren("*")!=null)
				return true;
			if(current.getChildren(string)!=null)
				current = current.getChildren(string);
			else
				return false;
		}
		return true;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		TreeOfNode abc = new TreeOfNode();
		abc.insert("*.abc.vn");
		System.out.println(abc.find("123.abc.vn"));
	}

}
