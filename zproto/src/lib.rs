pub mod proto {
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "lowercase")]
    pub enum Role {
        Tx,
        Rx,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    #[serde(rename_all = "lowercase")]
    pub enum ChanSel { L, R }

    /// Один канал устройства захвата, как его увидел агент. Имя и галочка
    /// живут на контроллере, не здесь.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct CaptureChan {
        pub id: String,
        pub device_key: String,
        pub device_name: String,
        pub index: u16,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct ChannelMap {
        pub out_l: Option<ChanSel>,
        pub out_r: Option<ChanSel>,
    }
    impl Default for ChannelMap {
        fn default() -> Self { Self { out_l: Some(ChanSel::L), out_r: Some(ChanSel::R) } }
    }

    /// Карта одного источника на ветке микшера колонки. `port` — порт этого tx.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub struct BranchChannelMap {
        pub port: u16,
        pub map: ChannelMap,
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "lowercase")]
    pub enum PipelineState {
        Stopped,
        Playing,
        Error,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct RoleTx {
        pub state: PipelineState,
        pub rms_l: f64,
        pub rms_r: f64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct RoleRx {
        pub state: PipelineState,
        pub dropped: u64,
        pub rendered: u64,
        pub queue_ms: i64,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    pub struct Telemetry {
        /// tx-половина; None (JSON null) — у узла нет роли tx.
        pub tx: Option<RoleTx>,
        /// rx-половина; None (JSON null) — у узла нет роли rx.
        pub rx: Option<RoleRx>,
        pub ptp_offset_ns: i64,
        pub uptime_s: u64,
    }

    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub enum EventKind {
        PipelineError,
        Underrun,
        Restarted,
    }

    /// Канал, которым узел держит сессию с контроллером.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(rename_all = "snake_case")]
    pub enum LinkKind {
        Wifi,
        Wired,
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(tag = "type")]
    pub enum AgentMsg {
        Hello {
            node_id: String,
            name: String,
            roles: Vec<Role>,
            version: String,
            capture_device: Option<String>,
            playback_device: Option<String>,
            /// Каналы захвата, которые агент видит на узле. Пусто у старых
            /// агентов и у узлов без роли tx.
            #[serde(default)]
            inputs: Vec<CaptureChan>,
            /// Каналы воспроизведения. Пусто у старых агентов и у узлов без роли rx.
            #[serde(default)]
            outputs: Vec<CaptureChan>,
            /// TX на PipeWire умеет поднять виртуальный выход. Старые агенты поле не шлют.
            #[serde(default)]
            supports_virtual_output: bool,
            /// Канал сессии. Старые агенты поле не шлют.
            #[serde(default)]
            link: Option<LinkKind>,
        },
        Telemetry(Telemetry),
        Event {
            kind: EventKind,
            detail: String,
        },
        /// Свежий список входов. Повторный Hello по живому сокету контроллер игнорирует.
        Inputs { inputs: Vec<CaptureChan> },
    }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
    pub struct Endpoint {
        pub ip: String,
        pub port: u16,
    }

    /// Одна зона передатчика: свой RTP, свои слушатели.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct ZoneEndpoints {
        pub zone_id: String,
        pub endpoints: Vec<Endpoint>,
        /// Имя PipeWire-источника этой зоны. None — общая голова (line-in).
        #[serde(default)]
        pub source: Option<String>,
    }

    /// Зона, для которой на передатчике нужен свой виртуальный выход.
    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
    pub struct VirtualZone {
        pub zone_id: String,
        pub name: String,
    }

    /// Секрет в сообщениях контроллера. В Debug не попадает.
    #[derive(Clone, Serialize, Deserialize, PartialEq, Eq)]
    #[serde(transparent)]
    pub struct SecretString(pub String);

    impl std::fmt::Debug for SecretString {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("<скрыт>")
        }
    }

    fn default_jb_ms() -> u32 { 2 }

    #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
    #[serde(tag = "type")]
    pub enum CtlMsg {
        Start,
        Stop,
        SetParam {
            jb_ms: Option<u32>,
            rx_buffer_us: Option<u32>,
            rx_latency_us: Option<u32>,
        },
        SetEndpoints {
            zones: Vec<ZoneEndpoints>,
            dscp: u8,
        },
        /// Порты, которые колонка складывает в один выход. Пустой список — тишина.
        /// `jb_ms` — запас rtpjitterbuffer. Старое сообщение без поля читается как 2.
        SetMix {
            ports: Vec<u16>,
            #[serde(default = "default_jb_ms")]
            jb_ms: u32,
        },
        SetChannelMap {
            map: ChannelMap,
            /// Пусто — `map` на выходе микшера, как раньше.
            /// Иначе у каждого порта своя карта до сложения, а `map` — сквозное стерео.
            #[serde(default)]
            branches: Vec<BranchChannelMap>,
        },
        ControllerMoved { addr: String },
        BecomeMaster { state: String },
        /// Откат трансфера: агент гасит локальный контроллер (systemctl
        /// disable --now) и снимает маркер преемника.
        DisableController,
        /// Создать или убрать виртуальные выходы. Пустой список — снять все.
        /// Имена в апплете: «Zvucast: {name}».
        SetVirtualZones { zones: Vec<VirtualZone> },
        /// Spotify Connect на каждую зону. `api_key: None` — приёмники не запускать.
        /// Один ключ, один играющий поток: пока устройство выбрано в Spotify,
        /// зона слышит только его.
        SetSoloist {
            api_key: Option<SecretString>,
            zones: Vec<VirtualZone>,
        },
        /// Sendspin-колонка на каждую зону передатчика. Пустой список гасит всех.
        /// Ключа нет: зоны и есть колонки.
        SetSendspin { zones: Vec<VirtualZone> },
        /// Создать или убрать виртуальный выход zvucast.
        SetVirtualOutput { enabled: bool },
        /// Куда смотрит pipewiresrc. None — источник по умолчанию.
        SetCaptureTarget { device_key: Option<String> },
    }
}

pub use proto::*;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hello_roundtrip() {
        let m = AgentMsg::Hello {
            node_id: "pc-1".into(),
            name: "pc".into(),
            roles: vec![Role::Tx, Role::Rx],
            version: "0.1.0".into(),
            capture_device: Some("plughw:CARD=Generic_1".into()),
            playback_device: Some("plughw:CARD=Generic_1".into()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
        };
        let s = serde_json::to_string(&m).unwrap();
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
        assert!(s.contains("\"roles\":[\"tx\",\"rx\"]"), "roles — массив: {s}");
    }

    #[test]
    fn hello_without_virtual_support_defaults_false() {
        let raw = r#"{"type":"Hello","node_id":"n","name":"pc","roles":["tx"],"version":"0.1.0","capture_device":null,"playback_device":null,"inputs":[],"outputs":[]}"#;
        let AgentMsg::Hello { supports_virtual_output, link, .. } = serde_json::from_str(raw).unwrap() else {
            panic!("не hello");
        };
        assert!(!supports_virtual_output);
        assert_eq!(link, None);
    }

    #[test]
    fn virtual_output_messages_roundtrip() {
        let inputs = AgentMsg::Inputs {
            inputs: vec![CaptureChan {
                id: "zvucast:0".into(),
                device_key: "zvucast".into(),
                device_name: "Zvucast".into(),
                index: 0,
            }],
        };
        let s = serde_json::to_string(&inputs).unwrap();
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), inputs);

        let on = CtlMsg::SetVirtualOutput { enabled: true };
        assert_eq!(serde_json::from_str::<CtlMsg>(&serde_json::to_string(&on).unwrap()).unwrap(), on);

        let target = CtlMsg::SetCaptureTarget { device_key: Some("zvucast".into()) };
        assert_eq!(serde_json::from_str::<CtlMsg>(&serde_json::to_string(&target).unwrap()).unwrap(), target);

        let clear = CtlMsg::SetCaptureTarget { device_key: None };
        assert_eq!(serde_json::from_str::<CtlMsg>(&serde_json::to_string(&clear).unwrap()).unwrap(), clear);
    }

    #[test]
    fn hello_minimal_devices_null() {
        let m = AgentMsg::Hello {
            node_id: "test".into(),
            name: "opi-rx2".into(),
            roles: vec![Role::Rx],
            version: "0.1.0".into(),
            capture_device: None,
            playback_device: Some("plughw:CARD=Codec".into()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"capture_device\":null"), "нет устройства — null: {s}");
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
    }

    #[test]
    fn telemetry_per_role_roundtrip() {
        let m = AgentMsg::Telemetry(Telemetry {
            tx: Some(RoleTx { state: PipelineState::Playing, rms_l: -20.1, rms_r: -20.3 }),
            rx: None,
            ptp_offset_ns: -15_000,
            uptime_s: 61,
        });
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"rx\":null"), "отсутствующая роль — null: {s}");
        assert!(s.contains("\"rms_l\":-20.1"), "поля tx на месте: {s}");
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
    }

    #[test]
    fn telemetry_both_roles_roundtrip() {
        let m = AgentMsg::Telemetry(Telemetry {
            tx: Some(RoleTx { state: PipelineState::Playing, rms_l: -9.5, rms_r: -9.4 }),
            rx: Some(RoleRx { state: PipelineState::Playing, dropped: 0, rendered: 12345, queue_ms: 5 }),
            ptp_offset_ns: 100,
            uptime_s: 7,
        });
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"queue_ms\":5"), "поля rx на месте: {s}");
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
    }

    #[test]
    fn ctl_roundtrip() {
        for m in [CtlMsg::Start, CtlMsg::Stop, CtlMsg::DisableController,
                  CtlMsg::SetParam { jb_ms: Some(5), rx_buffer_us: None, rx_latency_us: None }] {
            let s = serde_json::to_string(&m).unwrap();
            assert_eq!(serde_json::from_str::<CtlMsg>(&s).unwrap(), m);
        }
    }

    #[test]
    fn set_mix_roundtrip() {
        let m = CtlMsg::SetMix { ports: vec![5004, 5010], jb_ms: 40 };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"type\":\"SetMix\""));
        assert!(s.contains("\"ports\":[5004,5010]"));
        assert!(s.contains("\"jb_ms\":40"));
        assert_eq!(serde_json::from_str::<CtlMsg>(&s).unwrap(), m);
    }

    #[test]
    fn set_mix_without_jb_ms_defaults_to_2() {
        let raw = r#"{"type":"SetMix","ports":[5004]}"#;
        assert_eq!(
            serde_json::from_str::<CtlMsg>(raw).unwrap(),
            CtlMsg::SetMix { ports: vec![5004], jb_ms: 2 }
        );
    }

    #[test]
    fn event_kind_snake_case() {
        let m = AgentMsg::Event { kind: EventKind::PipelineError, detail: "x".into() };
        assert!(serde_json::to_string(&m).unwrap().contains("pipeline_error"));
    }

    #[test]
    fn hello_link_roundtrip() {
        let m = AgentMsg::Hello {
            node_id: "u-1".into(),
            name: "opi".into(),
            roles: vec![Role::Rx],
            version: "0.1".into(),
            capture_device: None,
            playback_device: None,
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: Some(LinkKind::Wifi),
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"link\":\"wifi\""));
        assert_eq!(serde_json::from_str::<AgentMsg>(&s).unwrap(), m);
        let raw = s.replace("\"link\":\"wifi\"", "\"link\":\"wired\"");
        let AgentMsg::Hello { link, .. } = serde_json::from_str(&raw).unwrap() else {
            panic!("не hello");
        };
        assert_eq!(link, Some(LinkKind::Wired));
    }

    #[test]
    fn hello_carries_node_id() {
        let m = AgentMsg::Hello {
            node_id: "u-1".into(), name: "opi".into(), roles: vec![Role::Rx],
            version: "0.1".into(), capture_device: None,
            playback_device: Some("plughw:CARD=Codec".into()),
            inputs: vec![],
            outputs: vec![],
            supports_virtual_output: false,
            link: None,
        };
        let v: serde_json::Value = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
        assert_eq!(v["node_id"], "u-1");
        let back: AgentMsg = serde_json::from_value(v).unwrap();
        let AgentMsg::Hello { node_id, .. } = back else { panic!("не hello") };
        assert_eq!(node_id, "u-1");
    }

    #[test]
    fn channel_map_roundtrip_and_default() {
        assert_eq!(ChannelMap::default(),
                   ChannelMap { out_l: Some(ChanSel::L), out_r: Some(ChanSel::R) });
        let cm = ChannelMap { out_l: Some(ChanSel::R), out_r: None };
        let s = serde_json::to_string(&cm).unwrap();
        assert_eq!(serde_json::from_str::<ChannelMap>(&s).unwrap(), cm);
        let v: serde_json::Value = serde_json::from_str(&s).unwrap();
        assert_eq!(v["out_l"], "r");      // serde lowercase
        assert!(v["out_r"].is_null());
    }

    #[test]
    fn new_ctl_msgs_roundtrip() {
        for m in [
            CtlMsg::SetChannelMap {
                map: ChannelMap { out_l: Some(ChanSel::L), out_r: None },
                branches: vec![],
            },
            CtlMsg::ControllerMoved { addr: "ws://192.0.2.207:8080/ws".into() },
            CtlMsg::BecomeMaster { state: "{\"version\":1}".into() },
        ] {
            let s = serde_json::to_string(&m).unwrap();
            let back: CtlMsg = serde_json::from_str(&s).unwrap();
            assert_eq!(format!("{back:?}"), format!("{m:?}"));
        }
    }

    #[test]
    fn set_endpoints_roundtrip() {
        let m = CtlMsg::SetEndpoints {
            zones: vec![
                ZoneEndpoints {
                    zone_id: "зал".into(),
                    endpoints: vec![Endpoint { ip: "192.0.2.36".into(), port: 5004 }],
                    source: None,
                },
                ZoneEndpoints {
                    zone_id: "спальня".into(),
                    endpoints: vec![Endpoint { ip: "192.0.2.99".into(), port: 5004 }],
                    source: None,
                },
            ],
            dscp: 40,
        };
        let s = serde_json::to_string(&m).unwrap();
        assert!(s.contains("\"dscp\":40"));
        assert!(s.contains("\"zone_id\":\"зал\""));
        assert!(s.contains("192.0.2.36"));
        assert_eq!(serde_json::from_str::<CtlMsg>(&s).unwrap(), m);
        let without_source = r#"{"type":"SetEndpoints","zones":[{"zone_id":"зал","endpoints":[{"ip":"192.0.2.36","port":5004}]}],"dscp":40}"#;
        let CtlMsg::SetEndpoints { zones, .. } = serde_json::from_str(without_source).unwrap() else {
            panic!("не SetEndpoints");
        };
        assert_eq!(zones[0].source, None);
        let named = CtlMsg::SetVirtualZones {
            zones: vec![VirtualZone { zone_id: "z1".into(), name: "Зал".into() }],
        };
        assert_eq!(serde_json::from_str::<CtlMsg>(&serde_json::to_string(&named).unwrap()).unwrap(), named);
        let solo = CtlMsg::SetSoloist {
            api_key: Some(SecretString("secret".into())),
            zones: vec![VirtualZone { zone_id: "z1".into(), name: "Зал".into() }],
        };
        let solo_json = serde_json::to_string(&solo).unwrap();
        assert!(solo_json.contains("\"api_key\":\"secret\""));
        assert_eq!(format!("{solo:?}"), format!("{:?}", serde_json::from_str::<CtlMsg>(&solo_json).unwrap()));
        assert!(!format!("{solo:?}").contains("secret"));
        let spin = CtlMsg::SetSendspin {
            zones: vec![VirtualZone { zone_id: "z1".into(), name: "Зал".into() }],
        };
        let spin_json = serde_json::to_string(&spin).unwrap();
        assert!(spin_json.contains("\"type\":\"SetSendspin\""));
        assert!(spin_json.contains("\"zone_id\":\"z1\""));
        assert_eq!(serde_json::from_str::<CtlMsg>(&spin_json).unwrap(), spin);
        assert_eq!(
            serde_json::from_str::<CtlMsg>(r#"{"type":"SetSendspin","zones":[]}"#).unwrap(),
            CtlMsg::SetSendspin { zones: vec![] }
        );
        // совместимость: старые варианты не сломаны
        assert_eq!(serde_json::from_str::<CtlMsg>("{\"type\":\"Start\"}").unwrap(), CtlMsg::Start);
        assert_eq!(
            serde_json::from_str::<CtlMsg>(r#"{"type":"SetChannelMap","map":{"out_l":"l","out_r":"r"}}"#).unwrap(),
            CtlMsg::SetChannelMap { map: ChannelMap::default(), branches: vec![] },
        );
    }
}
