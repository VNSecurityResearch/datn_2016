/**
 * 
 */
package com.viettel.hostfilter;

import java.io.Serializable;
/**
 * @author THANHNT78
 *
 */
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;

/**
 * Represents an IP range based on an address/mask.
 * 
 * @author THANHNT78, using code snippets by Scott Plante, John Kugelman.
 */
public class IpFilter implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String args[]) throws UnknownHostException {
		IpFilter ipmask = new IpFilter();

		ipmask.add("192.168.20.32/24");
		System.out.println("Checking " + ipmask + "...");

		test(ipmask, "192.168.20.31 ", true);
		test(ipmask, "192.168.20.32 ", true);
		test(ipmask, "192.168.20.33 ", true);
		test(ipmask, "192.168.20.34 ", true);
		test(ipmask, "192.168.20.35 ", true);
		test(ipmask, "192.168.20.36 ", true);
		test(ipmask, "192.168.20.254", true);
		test(ipmask, "192.168.20.157", true);
		test(ipmask, "192.168.21.1  ", false);
		test(ipmask, "192.168.19.255", false);
		test(ipmask, "192.168.24.1  ", false);

		ipmask = new IpFilter();
		ipmask.add("192.168.20.32/31");
		System.out.println("Checking " + ipmask + "...");

		test(ipmask, "192.168.20.31 ", false);
		test(ipmask, "192.168.20.32 ", true);
		test(ipmask, "192.168.20.33 ", true);
		test(ipmask, "192.168.20.34 ", false);
		test(ipmask, "192.168.20.35 ", false);
		test(ipmask, "192.168.20.36 ", false);
		test(ipmask, "192.168.20.254", false);
		test(ipmask, "192.168.20.157", false);
		test(ipmask, "192.168.21.1  ", false);
		test(ipmask, "192.168.19.255", false);
		test(ipmask, "192.168.24.1  ", false);

		ipmask = new IpFilter();
		ipmask.add("192.168.20.32/23");
		System.out.println("Checking " + ipmask + "...");

		test(ipmask, "192.168.20.31 ", true);
		test(ipmask, "192.168.20.32 ", true);
		test(ipmask, "192.168.20.33 ", true);
		test(ipmask, "192.168.20.254", true);
		test(ipmask, "192.168.21.254", true);
		test(ipmask, "192.168.19.255", false);
		test(ipmask, "192.168.24.1  ", false);

	}

	public static void test(IpFilter ipmask, String addr, boolean expect) throws UnknownHostException {
		boolean got = ipmask.matches(addr);
		System.out.println(addr + "\t(" + expect + ") ?\t" + got + "\t" + (got == expect ? "" : "!!!!!!!!"));
	}

	// private Inet4Address i4addr;
	// private byte maskCtr;

	private ArrayList<Ip> addrs = new ArrayList<Ip>();

	public IpFilter() {
	}

	/**
	 * IpFilter factory method.
	 * 
	 * @param addrSlashMask
	 *            IP/Mask String in format "nnn.nnn.nnn.nnn/mask". If the
	 *            "/mask" is omitted, "/32" (just the single address) is
	 *            assumed.
	 * @return void
	 * @throws UnknownHostException
	 *             if address part cannot be parsed by InetAddress
	 */
	public void add(String addrSlashMask) throws UnknownHostException {
		int pos = addrSlashMask.indexOf('/');
		String addr;
		byte maskCtr;
		if (pos == -1) {
			addr = addrSlashMask;
			maskCtr = 32;
		} else {
			addr = addrSlashMask.substring(0, pos);
			maskCtr = Byte.parseByte(addrSlashMask.substring(pos + 1));
		}
		addrs.add(new Ip(addrToInt((Inet4Address) InetAddress.getByName(addr)), ~((1 << (32 - maskCtr)) - 1)));
	}

	/**
	 * Test given IPv4 address against this IpFilter object.
	 * 
	 * @param testAddr
	 *            address to check.
	 * @return true if address is in the IP Mask range, false if not.
	 */
	public boolean matches(Inet4Address testAddr, int addrInt, int maskInt) {
		int testAddrInt = addrToInt(testAddr);
		return ((addrInt & maskInt) == (testAddrInt & maskInt));
	}

	/**
	 * Convenience method that converts String host to IPv4 address.
	 * 
	 * @param addr
	 *            IP address to match in nnn.nnn.nnn.nnn format or hostname.
	 * @return true if address is in the IP Mask range, false if not.
	 * @throws UnknownHostException
	 *             if the string cannot be decoded.
	 */
	public boolean matches(String addr) throws UnknownHostException {
		for (Ip ip : addrs) {
			if (matches((Inet4Address) InetAddress.getByName(addr), ip.addr, ip.mask))
				return true;
		}
		return false;
	}

	/**
	 * Converts IPv4 address to integer representation.
	 */
	private static int addrToInt(Inet4Address i4addr) {
		byte[] ba = i4addr.getAddress();
		return (ba[0] << 24) | ((ba[1] & 0xFF) << 16) | ((ba[2] & 0xFF) << 8) | (ba[3] & 0xFF);
	}
}

/**
 * A present a ip.
 */
class Ip implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * Ip class
	 */
	int addr = 0;
	int mask = 0;

	/**
	 * @param addr address
	 * @param mask mask
	 * */
	public Ip(int addr, int mask) {
		this.addr = addr;
		this.mask = mask;
	}
}
