package com.zzjj.consistency.utils;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import cn.hutool.core.util.ObjectUtil;

/**
 * 本机IP工具
 *
 * @author zengjin
 * @date 2023/11/19
 **/
public class NetUtils {

    private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(NetUtils.class);

    /**
     * 获取当前节点的地址信息
     *
     * @return 地址信息
     */
    public static String getCurrentPeerAddress() {
        try {
            Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
            while (allNetInterfaces.hasMoreElements()) {
                NetworkInterface netInterface = allNetInterfaces.nextElement();
                Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
                while (addresses.hasMoreElements()) {
                    InetAddress ip = addresses.nextElement();
                    if (!ObjectUtil.isEmpty(ip) && ip instanceof Inet4Address
                    // loopback地址即本机地址，IPv4的loopback范围是127.0.0.0 ~ 127.255.255.255
                        && !ip.isLoopbackAddress() && !ip.getHostAddress().contains(":")) {
                        return ip.getHostAddress();
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("获取本机ip地址时，发生异常", e);
        }
        return null;
    }

}