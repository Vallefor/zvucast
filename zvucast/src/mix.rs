//! Стабильный UDP-порт источника. Один tx — один порт на всех колонках,
//! чтобы два RTP не сходились в одном jitterbuffer.

pub const MIX_PORT_BASE: u16 = 5004;
pub const MIX_PORT_SLOTS: u16 = 64;

/// Слот имени до разрешения коллизий. 0..MIX_PORT_SLOTS.
pub fn mix_slot(name: &str) -> u16 {
    (fnv1a(name) % u32::from(MIX_PORT_SLOTS)) as u16
}

fn fnv1a(name: &str) -> u32 {
    let mut h: u32 = 0x811c_9dc5;
    for b in name.as_bytes() {
        h ^= u32::from(*b);
        h = h.wrapping_mul(0x0100_0193);
    }
    h
}

/// Порт каждого имени. Имена сортируются и дедупятся. Коллизия слота —
/// следующий свободный. Больше 64 имён — лишние (хвост сортировки) не входят.
/// Добавление имени, которое сортируется после уже назначенных, их порты не сдвигает.
pub fn assign_mix_ports(names: &[String]) -> Vec<(String, u16)> {
    let mut names: Vec<String> = names.to_vec();
    names.sort();
    names.dedup();
    let mut taken = [false; MIX_PORT_SLOTS as usize];
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let mut slot = mix_slot(&name);
        let mut placed = false;
        for _ in 0..MIX_PORT_SLOTS {
            if !taken[slot as usize] {
                taken[slot as usize] = true;
                out.push((name, MIX_PORT_BASE + slot * 2));
                placed = true;
                break;
            }
            slot = (slot + 1) % MIX_PORT_SLOTS;
        }
        if !placed {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    #[test]
    fn same_set_same_ports_regardless_of_order() {
        let names = vec!["pi4-tx".into(), "pc".into(), "pi3".into()];
        let mut reversed = names.clone();
        reversed.reverse();
        assert_eq!(assign_mix_ports(&names), assign_mix_ports(&reversed));
    }

    #[test]
    fn ports_are_unique_even_and_in_range() {
        let names: Vec<String> = ["pc", "pi3", "pi4-tx"].into_iter().map(str::to_string).collect();
        let got = assign_mix_ports(&names);
        assert_eq!(got.len(), 3);
        let mut seen = HashSet::new();
        for (_, port) in &got {
            assert!(*port >= MIX_PORT_BASE && *port <= MIX_PORT_BASE + (MIX_PORT_SLOTS - 1) * 2, "{port}");
            assert_eq!(port % 2, 0, "{port}");
            assert!(seen.insert(*port), "повтор порта {port}");
        }
    }

    #[test]
    fn name_sorted_last_does_not_move_existing_ports() {
        let names = vec!["pi4-tx".into(), "pc".into(), "pi3".into()];
        let before = assign_mix_ports(&names);
        let mut extra = names.clone();
        extra.push("zzzz-late".into());
        let after = assign_mix_ports(&extra);
        for (name, port) in &before {
            let kept = after.iter().find(|(n, _)| n == name).unwrap();
            assert_eq!(kept.1, *port, "{name}");
        }
        assert_eq!(after.len(), 4);
    }

    #[test]
    fn collision_takes_next_free_slot() {
        let first = "a".to_string();
        let slot = mix_slot(&first);
        let mut second = None;
        for i in 0..20_000 {
            let name = format!("c{i}");
            if mix_slot(&name) == slot && name.as_str() > first.as_str() {
                second = Some(name);
                break;
            }
        }
        let second = second.expect("не нашли коллизию слота");
        let got = assign_mix_ports(&[first.clone(), second.clone()]);
        let p1 = got.iter().find(|(n, _)| n == &first).unwrap().1;
        let p2 = got.iter().find(|(n, _)| n == &second).unwrap().1;
        assert_eq!(p1, MIX_PORT_BASE + slot * 2);
        assert_ne!(p1, p2);
        assert_eq!(p2 % 2, 0);
    }

    #[test]
    fn sixty_fifth_name_is_dropped() {
        let names: Vec<String> = (0..65).map(|i| format!("n{i:04}")).collect();
        let got = assign_mix_ports(&names);
        assert_eq!(got.len(), 64);
        let ports: HashSet<u16> = got.iter().map(|(_, p)| *p).collect();
        assert_eq!(ports.len(), 64);
    }
}
