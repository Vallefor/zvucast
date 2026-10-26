# Zvucast

Мультизональный звук по LAN.

Сборка: `just check-all`, `just build-pc`.

Установка на плату:

```bash
curl -fsSL https://raw.githubusercontent.com/Vallefor/zvucast/main/install.sh | bash
```

Бинарники лежат в GitHub Releases, а не в git.

Soloist ставится отдельно и в репозиторий не входит.

Деплой:

```bash
bash deploy/deploy.sh user@host ./zvucast examples/zvucast.example.toml
```
