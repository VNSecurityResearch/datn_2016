/**
 * 
 */
package com.viettel.hostfilter;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.regex.Pattern;

/**
 * A filter for ip,domain. 
 * It can use accurate addresses, regex, wildcard.
 * @author THANHNT78
 *
 */
public class FilterHost implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Pattern regex =null;
	private TreeOfNode domains = new TreeOfNode();
	private IpFilter ips = new IpFilter();
	private Pattern ipv4 = Pattern.compile("^\\d+\\.\\d+\\.\\d+\\.\\d+$");
	/**
	 * Create a filter.
	 * @param path path for file checklist.
	 * @throws IOException file not exist.
	 * */
	public FilterHost(String path) throws IOException{
		BufferedReader reader = new BufferedReader(new FileReader(path));
		String line;
		StringBuilder strRegexs = new StringBuilder("");
		
		while((line = reader.readLine())!=null){
			line.replaceAll("\\n", "");
			line.replaceAll("\\r", "");
			if(! line.equals("")){
				//if(line.charAt(0) =='#') continue;
				
				switch (line.charAt(0)) {
				case '#': 
					break;
				case '^':
					//regex.add(Pattern.compile(line.substring(1)));
					strRegexs.append("|"+line.substring(1));
					break;
				case '0':
				case '1':
				case '2':
				case '3':
				case '4':
				case '5':
				case '6':
				case '7':
				case '8':
				case '9':
					ips.add(line);
					break;
				default:
					domains.insert(line);
					break;
				}
			}
		}
		
		regex = Pattern.compile(strRegexs.toString(), Pattern.CASE_INSENSITIVE);
		reader.close();
	}
	/**
	 * find domain in check list.
	 * Return true if domain in checklist. Otherwise, return false
	 * @param domain it will be checked.
	 * @return boolean return true if domain in checklist. Otherwise, return false
	 * @throws UnknownHostException unknown host
	 * */
	public boolean check(String domain) throws UnknownHostException{
		if(ipv4.matcher(domain).matches()){
			if(ips.matches(domain)) return true;
		}
		else
		if( domains.find(domain)== true)
			return true;
		if(regex.matcher(domain).matches()) return true;
		return false;
	}
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		FilterHost abc = new FilterHost("WhiteList.txt");
		System.out.println(abc.check("abc.kcq-viettel.vn"));
		System.out.println(abc.check("1.1.1.1.in-addr.arpa"));
	}

	
}
