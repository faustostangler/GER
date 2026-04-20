# Rule 009: Storage Abstraction and GCP-Native Deployment Architecture

## 1. Context and Problem Statement

On 2026-04-19, a working tree change migrated `OUTPUT_FILE` from a local path (`gercon_consolidado.parquet`) to a MinIO/S3-backed URI (`s3://gercon/gercon_consolidado.parquet`). This introduced three silent failures that together prevented the analytics dashboard from loading entirely — with no Python exception, only a UI error banner.

This rule formalizes the architecture decision and the guardrails to prevent recurrence.

---

## 2. The Three Silent Failures (Post-Mortem)

### Failure 1 — `os.path.isfile("s3://...")` always returns `False`
**Location:** `app_analytics.py` fail-fast guard.

```python
# BUGADO — sempre False para URIs S3, app retornava antes de chamar o repositório:
if not os.path.isfile(settings.OUTPUT_FILE):
    st.error(...)
    return  # ← STOP. DuckDB nunca chamado.
```

**Why it's silent:** `os.path.isfile` não lança exceção para URIs S3 — simplesmente retorna `False`. O erro aparecia como "Parquet not found", mascarando que o arquivo existia no S3.

**Rule:** Para qualquer path que possa ser não-local (s3://, gs://, http://), **nunca use `os.path.isfile()` como guard**. Delegue a verificação ao adaptador responsável (repositório), que possui o contexto de autenticação e protocolo correto.

---

### Failure 2 — DuckDB `SET s3_endpoint` não aceita scheme `http://`
**Location:** `duckdb_repository.py` `__init__`.

```python
# BUGADO — DuckDB constrói http://http://minio%3A9000/... resultando em IO Error:
self.con.execute(f"SET s3_endpoint='{settings.s3.endpoint_url}';")
# settings.s3.endpoint_url = "http://minio:9000"

# CORRETO — strip do scheme via urlparse (ACL boundary S3Settings → DuckDB dialect):
parsed = urlparse(settings.s3.endpoint_url)
duckdb_endpoint = parsed.netloc or settings.s3.endpoint_url  # → "minio:9000"
self.con.execute(f"SET s3_endpoint='{duckdb_endpoint}';")
```

**Why it's silent:** DuckDB retorna `IOException: unable to connect` — não menciona URL malformada. O duplo `http://` só aparece em logs de rede de baixo nível.

**Rule:** `SET s3_endpoint` no DuckDB exige **apenas `host:port`**. Toda URL externa com scheme deve ter o scheme stripado via `urlparse().netloc` antes de ser passada a qualquer driver SQL que constrói URLs internamente. Este é um boundary ACL crítico.

---

### Failure 3 — `os.path.getmtime("s3://...")` lança `OSError`
**Location:** `duckdb_repository.py` `get_kpis()`.

```python
# BUGADO — capturado silenciosamente, sync_time=0.0, Amber Alert sempre ativo:
sync_time = os.path.getmtime(self.db_file)  # OSError para s3://
```

**Why it's silent:** A exceção era capturada no bloco `except Exception`, silenciando o erro e forçando `sync_time = 0.0` — que faz o Amber Alert disparar permanentemente, mesmo com dados frescos.

**Rule:** `os.path.getmtime` é válido **apenas para caminhos do filesystem local** (incluindo FUSE mounts como GCS). Para URIs de object storage, use o mecanismo nativo do adaptador (e.g., `MAX(dataCadastro)` via DuckDB para S3, ou `blob.updated` via GCS SDK).

---

## 3. Arquitetura de Storage: GCP-Native é a Decisão Correta

### Mapa definitivo de ambientes

| Ambiente | Storage | Como o DuckDB acessa |
|---|---|---|
| **Local (Docker Compose)** | Arquivo local bind-mounted | `os.path.isfile()` ✅ |
| **Cloud Run (Produção)** | GCS Bucket (FUSE volume mount) | `os.path.isfile()` ✅ |
| ~~MinIO (abandonado)~~ | ~~S3 URI~~ | ~~httpfs + urlparse (frágil)~~ |

### Por que GCS FUSE é superior ao S3/httpfs

O Cloud Run suporta **Cloud Storage Volumes** (`--add-volume=type=cloud-storage`):

```yaml
# .github/workflows/ci.yml — deploy-cloud-run
flags: >-
  --add-volume=name=datalake,type=cloud-storage,bucket=gercon-data-lake-prod-XXXXXXXXX
  --add-volume-mount=volume=datalake,mount-path=/app/data
env_vars: |
  OUTPUT_FILE=/app/data/gercon_consolidado.parquet  # path LOCAL normal
```

O GCS Bucket é montado como filesystem pelo Cloud Run runtime (FUSE). Do ponto de vista do Python, `OUTPUT_FILE` é sempre um **path local** — `os.path.isfile()`, `os.path.getmtime()`, e `duckdb.read_parquet()` funcionam sem modificação, sem boto3, sem httpfs, sem credenciais S3.

**Conclusão:** O ecossistema do projeto é **100% GCP**. Não há Amazon S3, não há boto3. MinIO foi descartado.

---

## 4. Regras Absolutas (Guardrails)

### R1 — Nunca adicionar `output_file.startswith("s3://")` branches no código de produção
O `OUTPUT_FILE` é sempre um path local (local dev = bind-mount, Cloud Run = GCS FUSE). Qualquer branch `s3://` é um code smell que indica vazamento de infraestrutura no domínio.

### R2 — `os.path.isfile()` e `os.path.getmtime()` são válidos para `OUTPUT_FILE`
Porque o OUTPUT_FILE é sempre um path local em todos os ambientes suportados.

### R3 — MinIO existe no Docker Compose para outros fins, não para o Parquet principal
O MinIO pode ser usado para artefatos auxiliares (exports, backups futuros), mas **nunca** como storage primário do dataset analítico. O dataset analítico segue o fluxo:

```
SQLite (raw_data.db) → sqlite_to_parquet.py → gercon_consolidado.parquet (local/GCS)
```

### R4 — Mudanças em `OUTPUT_FILE` requerem validação em todos os 3 pontos de uso
Qualquer alteração no valor ou formato de `settings.OUTPUT_FILE` deve ser verificada em:
1. `app_analytics.py` — guard `os.path.isfile()`
2. `duckdb_repository.py` — `__init__` (abertura) e `get_kpis()` (getmtime)
3. `sqlite_to_parquet.py` — destino de escrita do pipeline

### R5 — Nunca commitar `S3Settings` como dependência obrigatória em `AppSettings`
Se S3 for necessário no futuro (para artefatos auxiliares), deve ser uma configuração **opcional** com `default=None` e isolada em seu próprio adapter. O domínio e o repositório analítico principal não devem ter dependência de `S3Settings`.

### R6 — Persistência Local via Bind-Mount (MANDATÓRIO)
Para evitar que o dataset analítico desapareça após `rebuilds` (como observado em 20/04), o arquivo `gercon_consolidado.parquet` **deve** ser explicitamente listado como um bind-mount em `docker-compose.yml` para os serviços `analytics` e `worker`. 
- **Analytics**: Para leitura via DuckDB.
- **Worker**: Para persistência da saída do job `sqlite_to_parquet.py`.
- **Efeito**: Garante que o arquivo de 318MB sobreviva a remoções de containers e seja compartilhado instantaneamente entre serviços sem cópias de rede.

---

## 5. Checklist de Validação para Mudanças em Storage

Antes de qualquer PR que toque em `OUTPUT_FILE`, `config.py`, `duckdb_repository.py`, ou `app_analytics.py`:

- [ ] `uv run pytest tests/infrastructure/test_s3_duckdb_integration.py -v` — 15 regression tests passando
- [ ] `docker exec ger_analytics python -c "from infrastructure.config import settings; print(settings.OUTPUT_FILE)"` — nunca deve conter `s3://`
- [ ] `os.path.isfile(settings.OUTPUT_FILE)` retorna `True` dentro do container
- [ ] `os.path.getmtime(settings.OUTPUT_FILE)` não lança exceção dentro do container
- [ ] CI job `deploy-cloud-run` injeta `OUTPUT_FILE=/app/data/gercon_consolidado.parquet`

---

## 6. Diagnóstico Rápido — "Parquet database not found"

Quando a mensagem aparecer no browser, siga este protocolo **antes** de reiniciar qualquer serviço:

```bash
# 1. Verificar se o arquivo existe no container
docker exec ger_analytics ls -lh /app/gercon_consolidado.parquet

# 2. Verificar o valor real de OUTPUT_FILE em runtime
docker exec ger_analytics python -c "
import sys; sys.path.insert(0, '/app/src')
from infrastructure.config import settings
print('OUTPUT_FILE:', settings.OUTPUT_FILE)
import os; print('isfile:', os.path.isfile(settings.OUTPUT_FILE))
"

# 3. Se o arquivo existe mas isfile() retorna False → problema de path/schema
# 4. Se o arquivo não existe → rodar o pipeline:
docker exec ger_analytics python sqlite_to_parquet.py

# 5. Hard refresh no browser (Ctrl+Shift+R) antes de reiniciar containers
```

**`make down && make up-iam` é o ÚLTIMO recurso** — derruba Keycloak, MinIO e todo o stack desnecessariamente. O Streamlit recarrega automaticamente via bind-mount quando `app_analytics.py` ou `src/` são alterados.

---

## 7. Referências

- `app_analytics.py:94` — fail-fast guard `os.path.isfile()`
- `src/infrastructure/repositories/duckdb_repository.py:32` — `PRAGMA memory_limit` + abertura do Parquet
- `src/infrastructure/repositories/duckdb_repository.py:169` — `os.path.getmtime()` para `last_sync_at`
- `sqlite_to_parquet.py` — pipeline de consolidação SQLite → Parquet
- `.github/workflows/ci.yml` — `deploy-cloud-run` com `--add-volume=type=cloud-storage`
- `tests/infrastructure/test_s3_duckdb_integration.py` — 15 regression tests desta sessão
- ADR-001: Redis Distributed Cache (padrão de degradação graceful — padrão similar aplicado aqui)
