// SPDX-License-Identifier: WTFPL

//go:build android && cgo

package protect

/*
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/ioctl.h>
#include <ifaddrs.h>
#include <net/if.h>
#include <netinet/in.h>
#include <string.h>
#include <unistd.h>

// Linux interface flags (see <net/if.h>). Defined as raw constants because
// cgo does not reliably expose preprocessor macros across NDK versions.
enum {
	linux_IFF_UP          = 0x1,
	linux_IFF_BROADCAST   = 0x2,
	linux_IFF_LOOPBACK    = 0x8,
	linux_IFF_POINTOPOINT = 0x10,
	linux_IFF_RUNNING     = 0x40,
	linux_IFF_MULTICAST   = 0x1000,
};

static int ifc_get_mtu(const char *name, int *mtu) {
	struct ifreq ifr;
	int fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (fd < 0) return -1;
	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, name, IFNAMSIZ - 1);
	int r = ioctl(fd, SIOCGIFMTU, &ifr);
	close(fd);
	if (r < 0) return -1;
	*mtu = ifr.ifr_mtu;
	return 0;
}

static int ifc_get_hwaddr(const char *name, unsigned char *buf, int len) {
	if (len < 6) return -1;
	struct ifreq ifr;
	int fd = socket(AF_INET, SOCK_DGRAM, 0);
	if (fd < 0) return -1;
	memset(&ifr, 0, sizeof(ifr));
	strncpy(ifr.ifr_name, name, IFNAMSIZ - 1);
	int r = ioctl(fd, SIOCGIFHWADDR, &ifr);
	close(fd);
	if (r < 0) return -1;
	memcpy(buf, ifr.ifr_hwaddr.sa_data, 6);
	return 0;
}
*/
import "C"

import (
	"fmt"
	"net"
	"unsafe"

	"github.com/pion/transport/v4"
)

// loadInterfaces enumerates network interfaces via getifaddrs(3).
//
// On Android 11+ SELinux denies untrusted_app from binding
// netlink_route_socket (b/155595000). Both Go's net.Interfaces() and the
// anet library use AF_NETLINK internally, which triggers the denial.
// getifaddrs(3) uses /proc/net/dev + ioctl instead of netlink, so it is
// not affected by the SELinux restriction.
func loadInterfaces() ([]*transport.Interface, error) {
	var ifaHead *C.struct_ifaddrs
	if rc := C.getifaddrs(&ifaHead); rc != 0 {
		return nil, fmt.Errorf("getifaddrs: %w", ErrInterfacesUnavailable)
	}
	defer C.freeifaddrs(ifaHead)

	type ifEntry struct {
		iface net.Interface
		addrs []net.Addr
	}
	byName := make(map[string]*ifEntry)
	var order []string

	for p := ifaHead; p != nil; p = p.ifa_next {
		if p.ifa_name == nil {
			continue
		}
		name := C.GoString(p.ifa_name)
		if name == "" {
			continue
		}

		entry, ok := byName[name]
		if !ok {
			entry = &ifEntry{
				iface: net.Interface{
					Index: int(C.if_nametoindex(p.ifa_name)),
					Name:  name,
					Flags: mapIfFlags(C.uint(p.ifa_flags)),
				},
			}
			var mtu C.int
			if C.ifc_get_mtu(p.ifa_name, &mtu) == 0 && mtu > 0 {
				entry.iface.MTU = int(mtu)
			}
			var hwBuf [6]C.uchar
			if C.ifc_get_hwaddr(p.ifa_name, &hwBuf[0], 6) == 0 {
				entry.iface.HardwareAddr = C.GoBytes(unsafe.Pointer(&hwBuf[0]), 6)
			}
			byName[name] = entry
			order = append(order, name)
		}

		if p.ifa_addr == nil {
			continue
		}
		if addr := parseIfaddr(p.ifa_addr, p.ifa_netmask); addr != nil {
			entry.addrs = append(entry.addrs, addr)
		}
	}

	out := make([]*transport.Interface, 0, len(order))
	for _, name := range order {
		entry := byName[name]
		ifc := transport.NewInterface(entry.iface)
		for _, addr := range entry.addrs {
			ifc.AddAddress(addr)
		}
		out = append(out, ifc)
	}
	return out, nil
}

func parseIfaddr(sa, mask *C.struct_sockaddr) net.Addr {
	if sa == nil {
		return nil
	}
	switch sa.sa_family {
	case C.AF_INET:
		return parseIPv4(sa, mask)
	case C.AF_INET6:
		return parseIPv6(sa, mask)
	}
	return nil
}

func parseIPv4(sa, mask *C.struct_sockaddr) net.Addr {
	sin := (*C.struct_sockaddr_in)(unsafe.Pointer(sa))
	ip := net.IP(C.GoBytes(unsafe.Pointer(&sin.sin_addr), 4))
	var maskIP net.IPMask
	if mask != nil && mask.sa_family == C.AF_INET {
		sinMask := (*C.struct_sockaddr_in)(unsafe.Pointer(mask))
		maskIP = net.IPMask(C.GoBytes(unsafe.Pointer(&sinMask.sin_addr), 4))
	}
	return &net.IPNet{IP: ip, Mask: maskIP}
}

func parseIPv6(sa, mask *C.struct_sockaddr) net.Addr {
	sin6 := (*C.struct_sockaddr_in6)(unsafe.Pointer(sa))
	ip := net.IP(C.GoBytes(unsafe.Pointer(&sin6.sin6_addr), 16))
	var maskIP net.IPMask
	if mask != nil && mask.sa_family == C.AF_INET6 {
		sin6Mask := (*C.struct_sockaddr_in6)(unsafe.Pointer(mask))
		maskIP = net.IPMask(C.GoBytes(unsafe.Pointer(&sin6Mask.sin6_addr), 16))
	}
	return &net.IPNet{IP: ip, Mask: maskIP}
}

func mapIfFlags(flags C.uint) net.Flags {
	var f net.Flags
	if flags&C.linux_IFF_UP != 0 {
		f |= net.FlagUp
	}
	if flags&C.linux_IFF_BROADCAST != 0 {
		f |= net.FlagBroadcast
	}
	if flags&C.linux_IFF_LOOPBACK != 0 {
		f |= net.FlagLoopback
	}
	if flags&C.linux_IFF_POINTOPOINT != 0 {
		f |= net.FlagPointToPoint
	}
	if flags&C.linux_IFF_MULTICAST != 0 {
		f |= net.FlagMulticast
	}
	if flags&C.linux_IFF_RUNNING != 0 {
		f |= net.FlagRunning
	}
	return f
}
