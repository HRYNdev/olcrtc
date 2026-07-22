// SPDX-License-Identifier: WTFPL

//go:build android && !cgo

package protect

import (
	"github.com/pion/transport/v4"
)

// loadInterfaces returns an error on android without cgo. The netlink-free
// getifaddrs path requires cgo; without it there is no reliable way to
// enumerate interface addresses under Android 11+ SELinux restrictions.
func loadInterfaces() ([]*transport.Interface, error) {
	return nil, ErrInterfacesUnavailable
}
