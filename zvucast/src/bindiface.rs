//! UDP-сокет, привязанный к имени интерфейса.
//! Ошибка привязки или `bind` наружу отдаёт `Err`: неготовый сокет в `UdpSocket` не конвертируем.

use std::net::{Ipv4Addr, SocketAddrV4};

use socket2::{Domain, Protocol, Socket, Type};

/// `SO_BINDTODEVICE`: сокет принимает пакеты только с этого интерфейса.
pub fn bind_device(sock: &Socket, iface: &str) -> std::io::Result<()> {
    sock.bind_device(Some(iface.as_bytes()))
}

/// Неблокирующий UDP на `ip:port`, уже привязанный к `iface`.
/// `SO_REUSEADDR` не ставим: на Linux два таких UDP-сокета делят порт,
/// и второй контроллер на этом хосте тоже ответит на `ZVUCAST_WHO_IS_MASTER?`.
/// Сбой до успешного `bind` и при переводе в неблокирующий режим возвращается наружу.
pub fn udp_bound(iface: &str, ip: Ipv4Addr, port: u16) -> std::io::Result<std::net::UdpSocket> {
    let sock = Socket::new(Domain::IPV4, Type::DGRAM, Some(Protocol::UDP))?;
    bind_device(&sock, iface)?;
    sock.bind(&SocketAddrV4::new(ip, port).into())?;
    sock.set_nonblocking(true)?;
    Ok(sock.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_iface_is_an_error() {
        let err = udp_bound("no-such-iface-zz", "192.0.2.1".parse().unwrap(), 0).unwrap_err();
        assert_ne!(err.kind(), std::io::ErrorKind::WouldBlock);
    }

    /// Порт эксклюзивен: второй контроллер на этом хосте не должен занять UDP 5005.
    #[test]
    fn bound_port_is_exclusive() {
        let ip = "127.0.0.1".parse().unwrap();
        let first = udp_bound("lo", ip, 0).unwrap();
        let port = first.local_addr().unwrap().port();
        let err = udp_bound("lo", ip, port).unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::AddrInUse);
    }
}
