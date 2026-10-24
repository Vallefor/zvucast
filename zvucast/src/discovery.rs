//! Discovery (UDP 5005): контроллер отвечает `ZVUCAST_MASTER` на broadcast-
//! запрос `ZVUCAST_WHO_IS_MASTER?`, агент перед каждой попыткой подключения
//! спрашивает, кто мастер, и при ответе переключается на найденный адрес.

use std::net::Ipv4Addr;
use std::time::Duration;
use tokio::net::UdpSocket;

pub const DISCOVERY_PORT: u16 = 5005;
pub const CONTROLLER_PORT: u16 = 8080;
pub const QUERY: &[u8] = b"ZVUCAST_WHO_IS_MASTER?";
pub const ANSWER: &[u8] = b"ZVUCAST_MASTER";

pub async fn responder(bind: &str) -> std::io::Result<()> {
    let sock = UdpSocket::bind(bind).await?;
    answer_loop(&sock).await
}

/// Сокет респондера: `0.0.0.0:5005` после `SO_BINDTODEVICE`.
/// Адрес линка в `bind` не подставляется — устройство задаёт имя интерфейса.
/// Несуществующий интерфейс падает на `bind_device`, порт ещё не занят.
pub fn responder_socket(iface: &str, ip: Ipv4Addr) -> std::io::Result<std::net::UdpSocket> {
    crate::bindiface::udp_bound(iface, Ipv4Addr::UNSPECIFIED, DISCOVERY_PORT)
        .map_err(|e| std::io::Error::new(e.kind(), format!("{iface} ({ip}): {e}")))
}

/// Отвечает на запрос мастера с привязанного сокета.
/// Сам `0.0.0.0` без устройства не открывает: ошибка привязки — сразу `Err`.
pub async fn serve_bound(iface: &str, ip: Ipv4Addr) -> std::io::Result<()> {
    let sock = open_bound(iface, ip)?;
    answer_loop(&sock).await
}

fn open_bound(iface: &str, ip: Ipv4Addr) -> std::io::Result<UdpSocket> {
    UdpSocket::from_std(responder_socket(iface, ip)?)
}

/// Слушает UDP 5005 только пока опубликован интерфейс.
/// `None` закрывает сокет. Новая публикация закрывает старый и открывает другой.
/// Предпроверку «уже есть мастер» не повторяет.
pub async fn serve_uplink(
    mut uplink: tokio::sync::watch::Receiver<Option<crate::uplink::ActiveLink>>,
) {
    loop {
        let link = uplink.borrow_and_update().clone();
        let Some(link) = link else {
            tracing::info!("discovery: активного интерфейса нет — UDP {DISCOVERY_PORT} молчит");
            if uplink.changed().await.is_err() {
                return;
            }
            continue;
        };
        let sock = match open_bound(&link.name, link.ipv4) {
            Ok(sock) => sock,
            Err(e) => {
                tracing::error!("discovery: {e} — 0.0.0.0 без привязки не слушаем");
                if !pause_or_link(&mut uplink).await {
                    return;
                }
                continue;
            }
        };
        tracing::info!(
            "discovery: слушаю {} ({}) :{DISCOVERY_PORT}",
            link.name,
            link.ipv4
        );
        if !answer_until_change(&sock, &mut uplink).await {
            return;
        }
        // Смена публикации или ошибка чтения: старый сокет закрыт до нового bind.
        drop(sock);
    }
}

/// Пауза перед повторным открытием того же линка. Смена публикации прерывает ожидание.
/// `false` — наблюдатель закрылся.
async fn pause_or_link(
    uplink: &mut tokio::sync::watch::Receiver<Option<crate::uplink::ActiveLink>>,
) -> bool {
    tokio::select! {
        _ = tokio::time::sleep(Duration::from_secs(1)) => true,
        changed = uplink.changed() => changed.is_ok(),
    }
}

async fn answer_loop(sock: &UdpSocket) -> std::io::Result<()> {
    let mut buf = [0u8; 64];
    loop {
        let (n, peer) = sock.recv_from(&mut buf).await?;
        reply(sock, &buf[..n], peer).await;
    }
}

/// `true` — сокет надо закрыть и открыть заново (или замолчать). `false` — канал закрыт.
async fn answer_until_change(
    sock: &UdpSocket,
    uplink: &mut tokio::sync::watch::Receiver<Option<crate::uplink::ActiveLink>>,
) -> bool {
    let mut buf = [0u8; 64];
    loop {
        tokio::select! {
            changed = uplink.changed() => return changed.is_ok(),
            recvd = sock.recv_from(&mut buf) => {
                match recvd {
                    Ok((n, peer)) => reply(sock, &buf[..n], peer).await,
                    Err(e) => {
                        tracing::warn!("discovery: чтение: {e}");
                        return pause_or_link(uplink).await;
                    }
                }
            }
        }
    }
}

async fn reply(sock: &UdpSocket, payload: &[u8], peer: std::net::SocketAddr) {
    if payload == QUERY {
        let _ = sock.send_to(ANSWER, peer).await;
    }
}

pub async fn query_once(dest: &str, timeout: Duration) -> std::io::Result<Option<String>> {
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.set_broadcast(true)?;
    sock.send_to(QUERY, dest).await?;
    let mut buf = [0u8; 64];
    match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
        Ok(Ok((n, from))) if &buf[..n] == ANSWER =>
            Ok(Some(format!("ws://{}:{}/ws", from.ip(), CONTROLLER_PORT))),
        _ => Ok(None),
    }
}

/// Поиск мастера с сокета, привязанного к выбранному интерфейсу.
/// `udp_bound` уже выставляет `O_NONBLOCK`; перед `send_to`/`recv_from`
/// std-сокет переводим в `tokio` через `from_std`. Ошибка привязки — `Err`,
/// непривязанный сокет не открываем.
pub async fn query_bound(
    dest: &str,
    timeout: Duration,
    iface: &str,
    ip: Ipv4Addr,
) -> std::io::Result<Option<String>> {
    let std_sock = crate::bindiface::udp_bound(iface, ip, 0)?;
    std_sock.set_broadcast(true)?;
    let sock = UdpSocket::from_std(std_sock)?;
    sock.send_to(QUERY, dest).await?;
    let mut buf = [0u8; 64];
    match tokio::time::timeout(timeout, sock.recv_from(&mut buf)).await {
        Ok(Ok((n, from))) if &buf[..n] == ANSWER =>
            Ok(Some(format!("ws://{}:{}/ws", from.ip(), CONTROLLER_PORT))),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn responder_answers_query() {
        let bind = "127.0.0.1:15005";
        let task = tokio::spawn(responder(bind));
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let got = query_once(bind, std::time::Duration::from_secs(1)).await.unwrap();
        assert_eq!(got.as_deref(), Some("ws://127.0.0.1:8080/ws"));
        task.abort();
    }

    #[tokio::test]
    async fn query_timeout_returns_none() {
        let got = query_once("127.0.0.1:15099", std::time::Duration::from_millis(150)).await.unwrap();
        assert!(got.is_none());
    }

    #[tokio::test]
    async fn query_bound_rejects_a_missing_iface() {
        let err = query_bound("127.0.0.1:9", std::time::Duration::from_millis(50), "no-such-iface-zz", "192.0.2.1".parse().unwrap()).await.unwrap_err();
        let _ = err;
    }

    #[tokio::test]
    async fn bound_responder_does_not_answer_without_a_socket() {
        // Пустой приёмник не отвечает. Функция serve_bound возвращает Err на missing iface
        // и не биндит 0.0.0.0 сама.
        let err = serve_bound("no-such-iface-zz", "192.0.2.1".parse().unwrap()).await.unwrap_err();
        let _ = err;
    }
}
