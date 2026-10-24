/* Soloist кладёт в mDNS первый попавшийся локальный адрес. На машине с
 * Docker это 172.17.0.1, и приложение Spotify в LAN такой колонки не видит.
 * ZV_IFACE — имя интерфейса из sync_interface: остальные адреса прячем.
 */
#define _GNU_SOURCE
#include <ifaddrs.h>
#include <dlfcn.h>
#include <string.h>
#include <stdlib.h>

int getifaddrs(struct ifaddrs **ifap) {
    static int (*real_getifaddrs)(struct ifaddrs **) = NULL;
    if (!real_getifaddrs)
        real_getifaddrs = dlsym(RTLD_NEXT, "getifaddrs");
    int rc = real_getifaddrs(ifap);
    if (rc != 0 || !ifap || !*ifap)
        return rc;
    const char *keep = getenv("ZV_IFACE");
    if (!keep || !*keep)
        return rc;
    for (struct ifaddrs *cur = *ifap; cur; cur = cur->ifa_next) {
        if (cur->ifa_name && strcmp(cur->ifa_name, keep) != 0) {
            cur->ifa_addr = NULL;
            cur->ifa_netmask = NULL;
            cur->ifa_broadaddr = NULL;
        }
    }
    return 0;
}
