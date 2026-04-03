"""
Domain Mapper: Event Sourcing & Ping-Pong SLA Engine.

Transforma o payload JSON do Vendor (Gercon/Procempa) em um dicionário plano
para persistência em Parquet. A verdade imutável é extraída do snapshot
`entidade` dentro da primeira evolução cronológica (CRIACAO).

O Motor SLA (Ping-Pong) rastreia a posse temporal entre Solicitante e
Regulador para cálculo preciso de Lead Times em dias.
"""

from typing import Dict, Any
from datetime import datetime
import json
import unicodedata
import hashlib


# ---------------------------------------------------------------------------
# Helpers (DRY & SRE Type Safety)
# ---------------------------------------------------------------------------


def hash_pii(text: Any) -> str:
    """Aplica Salting e SHA-256 unidirecional para anonimizar PII."""
    if not text:
        return ""
    salt = "g3rc0N_@n0n!"
    val = str(text).strip().lower() + salt
    return hashlib.sha256(val.encode("utf-8")).hexdigest()


def remove_accents(input_str: str) -> str:
    """Remove diacríticos para matching resiliente de labels legados."""
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def timestamp_to_date(ts: Any) -> str:
    """Converte epoch ms para string formatada dd/mm/YYYY HH:MM:SS."""
    if not ts:
        return ""
    try:
        dt = datetime.fromtimestamp(float(ts) / 1000.0)
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except Exception:
        return ""


def safe_bool(val: Any) -> bool:
    """SRE: Garante inferência BOOLEAN nativa no DuckDB/Parquet."""
    if val is None:
        return False
    if isinstance(val, bool):
        return val
    if isinstance(val, str):
        return val.strip().lower() in ["true", "1", "yes", "sim", "s"]
    return bool(val)


def extract_unidade(unidade_dict: dict, prefix: str, target_dict: dict) -> None:
    """Helper DRY: extrai Unit demographics em chaves planas no target_dict."""
    if not isinstance(unidade_dict, dict):
        return
    # pragma: no mutate
    target_dict[f"{prefix}_nome"] = unidade_dict.get("nome", "")  # pragma: no mutate
    target_dict[f"{prefix}_razaoSocial"] = unidade_dict.get("razaoSocial", "")  # pragma: no mutate
    tipo_un = unidade_dict.get("tipoUnidade") or {}
    target_dict[f"{prefix}_tipoUnidade_descricao"] = tipo_un.get("descricao", "")  # pragma: no mutate
    mun = unidade_dict.get("municipio") or {}
    target_dict[f"{prefix}_municipio_nome"] = mun.get("nome", "")  # pragma: no mutate
    target_dict[f"{prefix}_municipio_uf"] = mun.get("uf", "")  # pragma: no mutate


def _parse_detalhes(det_raw: Any) -> dict:
    """Parsing defensivo do JSON 'detalhes' de uma evolução."""
    if isinstance(det_raw, dict):
        return det_raw
    if isinstance(det_raw, str):
        try:
            stripped = det_raw.strip()
            if stripped.startswith("{"):
                return json.loads(stripped)
        except Exception:
            pass
    return {}


# ---------------------------------------------------------------------------
# Schema (Linguagem Ubíqua — Clean Break, sem aliases legados)
# ---------------------------------------------------------------------------

COLUNAS = [
    # 1. Identificadores Raiz
    "numeroCMCE",
    "situacao",
    "origem_lista",
    "corRegulador",
    "dataSolicitacao",
    "liminarOrdemJudicial",
    # 2. Demografia do Paciente (usuarioSUS)
    "usuarioSUS_nomeCompleto",
    "usuarioSUS_dataNascimento",
    "usuarioSUS_sexo",
    "usuarioSUS_racaCor",
    "usuarioSUS_cpf",
    "usuarioSUS_nomeMae",
    "usuarioSUS_cartaoSus",
    "usuarioSUS_nacionalidade",
    "usuarioSUS_logradouro",
    "usuarioSUS_numero",
    "usuarioSUS_complemento",
    "usuarioSUS_bairro",
    "usuarioSUS_cep",
    "usuarioSUS_municipioResidencia_nome",
    "usuarioSUS_municipioResidencia_uf",
    # 3. Atores Raiz (Governança)
    "operador_nome",
    "operador_cpf",
    "usuarioSolicitante_nome",
    "usuarioSolicitante_cpf",
    # 4. Ciclo de Vida Raiz
    "dataPrimeiroAgendamento",
    "dataPrimeiraAutorizacao",
    "regularizacaoAcesso",
    "statusProvisorio",
    "justificativaRetorno",
    "justificativaDuplicacao",
    "motivoPendencia",
    "motivoCancelamento",
    "motivoEncerramento",
    "descricaoEncerramento",
    # 5. Snapshot Imutável (entidade — extraído da 1ª evolução CRIACAO)
    "entidade_sistemaOrigem",
    "entidade_complexidade",
    "entidade_semClassificacao",
    "entidade_cidPrincipal_codigo",
    "entidade_cidPrincipal_descricao",
    "entidade_especialidade_descricao",
    "entidade_especialidade_descricaoAuxiliar",
    "entidade_especialidade_cbo_descricao",
    "entidade_especialidade_especialidadeMae_descricao",
    "entidade_especialidade_especialidadeMae_cbo_descricao",
    "entidade_especialidade_tipoRegulacao",
    "entidade_especialidade_teleconsulta",
    "entidade_especialidade_ativa",
    "entidade_especialidade_matriciamento",
    "entidade_especialidade_tipoOCI",
    "entidade_classificacaoRisco_totalPontos",
    "entidade_classificacaoRisco_pontosGravidade",
    "entidade_classificacaoRisco_pontosTempo",
    "entidade_classificacaoRisco_cor",
    "entidade_classificacaoRisco_reclassificadaSolicitante",
    "entidade_foraDaRegionalizacao",
    "entidade_possuiDita",
    # 6. Geografias do Snapshot (via extract_unidade)
    "entidade_municipioUsuarioSUS_nome",
    "entidade_municipioUsuarioSUS_uf",
    "entidade_unidadeOperador_nome",
    "entidade_unidadeOperador_razaoSocial",
    "entidade_unidadeOperador_tipoUnidade_descricao",
    "entidade_unidadeOperador_municipio_nome",
    "entidade_unidadeOperador_municipio_uf",
    "entidade_unidadeOperador_centralRegulacao_nome",
    "entidade_unidadeOperador_centralRegulacao_razaoSocial",
    "entidade_unidadeOperador_centralRegulacao_tipoUnidade_descricao",
    "entidade_unidadeOperador_centralRegulacao_municipio_nome",
    "entidade_unidadeOperador_centralRegulacao_municipio_uf",
    "entidade_unidadeReferencia_nome",
    "entidade_unidadeReferencia_razaoSocial",
    "entidade_unidadeReferencia_tipoUnidade_descricao",
    "entidade_unidadeReferencia_municipio_nome",
    "entidade_unidadeReferencia_municipio_uf",
    "entidade_unidadeReferencia_centralRegulacao_nome",
    "entidade_unidadeReferencia_centralRegulacao_razaoSocial",
    "entidade_unidadeReferencia_centralRegulacao_tipoUnidade_descricao",
    "entidade_unidadeReferencia_centralRegulacao_municipio_nome",
    "entidade_unidadeReferencia_centralRegulacao_municipio_uf",
    "entidade_centralRegulacao_nome",
    "entidade_centralRegulacao_razaoSocial",
    "entidade_centralRegulacao_tipoUnidade_descricao",
    "entidade_centralRegulacao_municipio_nome",
    "entidade_centralRegulacao_municipio_uf",
    # 7. SLA Metrics (Ping-Pong Engine V2 — State Machine)
    "SLA_Tempo_Solicitante_Dias",
    "SLA_Tempo_Regulador_Dias",
    "SLA_Lead_Time_Total_Dias",
    "SLA_Interacoes_Regulacao",
    "SLA_Atores_Envolvidos",
    "SLA_Desfecho_Atingido",
    "SLA_Tipo_Desfecho",
    "SLA_Marco_Autorizada",
    "SLA_Marco_Agendada",
    "SLA_Marco_Realizada",
    # 8. Campos Computados (derivados das evoluções)
    "dataCadastro",
    "medicoSolicitante",
    # 9. Dual-Write das Evoluções (UI Text + Data Lake JSON)
    "historico_quadro_clinico",
    "historico_evolucoes_completo",
    "evolucoes_json",
]


def clean_data_row(data: Dict[str, Any]) -> Dict[str, Any]:
    """Sanitiza e projeta o dicionário nas colunas canônicas do schema."""
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = (  # pragma: no mutate
                v.replace("\r\n", " ").replace("\r", " ").replace("\n", "  ")
            )
        else:
            cleaned_row[col] = v  # pragma: no mutate
    return cleaned_row


# ---------------------------------------------------------------------------
# Core Domain Logic: Event Sourcing Flattener
# ---------------------------------------------------------------------------


def flatten_solicitacao(j_dict: Dict[Any, Any], origem_lista: str) -> Dict[str, Any]:
    """
    Transforma o JSON complexo do Vendor em linha plana para Parquet.

    Arquitetura Event Sourcing:
    1. Extrai campos raiz mutáveis (demographics, lifecycle).
    2. Extrai snapshot imutável de `entidade` da 1ª evolução (CRIACAO).
    3. Calcula métricas SLA via Ping-Pong State Machine.
    """
    data: Dict[str, Any] = {}

    # pragma: no mutate
    # ── 1. IDENTIFICADORES RAIZ ──────────────────────────────────────────
    data["numeroCMCE"] = j_dict.get("numeroCMCE", "")  # pragma: no mutate
    data["situacao"] = j_dict.get("situacao", "")  # pragma: no mutate
    data["origem_lista"] = origem_lista  # pragma: no mutate
    data["corRegulador"] = j_dict.get("corRegulador", "")  # pragma: no mutate
    data["dataSolicitacao"] = timestamp_to_date(j_dict.get("dataSolicitacao"))  # pragma: no mutate
    data["liminarOrdemJudicial"] = j_dict.get("liminarOrdemJudicial", "")  # pragma: no mutate

    # ── 2. DEMOGRAFIA DO PACIENTE (usuarioSUS) ───────────────────────────
    # pragma: no mutate
    u_sus = j_dict.get("usuarioSUS") or {}
    data["usuarioSUS_nomeCompleto"] = hash_pii(u_sus.get("nomeCompleto", ""))  # pragma: no mutate
    dn_raw = timestamp_to_date(u_sus.get("dataNascimento"))
    data["usuarioSUS_dataNascimento"] = dn_raw.split(" ")[0] if dn_raw else ""  # pragma: no mutate
    data["usuarioSUS_sexo"] = u_sus.get("sexo", "")  # pragma: no mutate
    data["usuarioSUS_racaCor"] = u_sus.get("racaCor", "")  # pragma: no mutate
    data["usuarioSUS_cpf"] = hash_pii(u_sus.get("cpf", ""))  # pragma: no mutate
    data["usuarioSUS_nomeMae"] = hash_pii(u_sus.get("nomeMae", ""))  # pragma: no mutate
    data["usuarioSUS_cartaoSus"] = hash_pii(u_sus.get("cartaoSus", ""))  # pragma: no mutate
    data["usuarioSUS_nacionalidade"] = u_sus.get("nacionalidade", "")  # pragma: no mutate
    data["usuarioSUS_logradouro"] = u_sus.get("logradouro", "")  # pragma: no mutate
    data["usuarioSUS_numero"] = u_sus.get("numero", "")  # pragma: no mutate
    data["usuarioSUS_complemento"] = u_sus.get("complemento", "")  # pragma: no mutate
    data["usuarioSUS_bairro"] = u_sus.get("bairro", "")  # pragma: no mutate
    data["usuarioSUS_cep"] = u_sus.get("cep", "")  # pragma: no mutate
    mun_res = u_sus.get("municipioResidencia") or {}
    data["usuarioSUS_municipioResidencia_nome"] = mun_res.get("nome", "")  # pragma: no mutate
    data["usuarioSUS_municipioResidencia_uf"] = mun_res.get("uf", "")  # pragma: no mutate

    # ── 3. ATORES RAIZ (Governança) ──────────────────────────────────────
    # pragma: no mutate
    op = j_dict.get("operador") or {}
    op_prof = op.get("profissional") or {}
    data["operador_nome"] = op.get("nome") or op_prof.get("nome", "")  # pragma: no mutate
    data["operador_cpf"] = hash_pii(op.get("cpf") or op_prof.get("cpf", ""))  # pragma: no mutate

    us = j_dict.get("usuarioSolicitante") or {}
    us_prof = us.get("profissional") or {}
    data["usuarioSolicitante_nome"] = us.get("nome") or us_prof.get("nome", "")  # pragma: no mutate
    data["usuarioSolicitante_cpf"] = hash_pii(us.get("cpf") or us_prof.get("cpf", ""))  # pragma: no mutate

    # ── 4. CICLO DE VIDA RAIZ ────────────────────────────────────────────
    # pragma: no mutate
    data["dataPrimeiroAgendamento"] = timestamp_to_date(  # pragma: no mutate
        j_dict.get("dataPrimeiroAgendamento")
    )
    data["dataPrimeiraAutorizacao"] = timestamp_to_date(  # pragma: no mutate
        j_dict.get("dataPrimeiraAutorizacao")
    )
    data["regularizacaoAcesso"] = j_dict.get("regularizacaoAcesso", "")  # pragma: no mutate
    data["statusProvisorio"] = j_dict.get("statusProvisorio", "")  # pragma: no mutate
    data["justificativaRetorno"] = j_dict.get("justificativaRetorno", "")  # pragma: no mutate
    data["justificativaDuplicacao"] = j_dict.get("justificativaDuplicacao", "")  # pragma: no mutate
    data["motivoPendencia"] = j_dict.get("motivoPendencia", "")  # pragma: no mutate
    data["motivoCancelamento"] = j_dict.get("motivoCancelamento", "")  # pragma: no mutate
    data["motivoEncerramento"] = j_dict.get("motivoEncerramento", "")  # pragma: no mutate
    data["descricaoEncerramento"] = j_dict.get("descricaoEncerramento", "")  # pragma: no mutate

    # ── 5-7. EVENT SOURCING: Evoluções + Snapshot + SLA ──────────────────
    evolucoes = j_dict.get("evolucoes", [])
    # Fail-safe: coerce to int para prevenir TypeError em NoneType
    evolucoes_ordenadas = sorted(evolucoes, key=lambda x: int(x.get("data") or 0))

    # SLA State Machine Variables
    tempo_solicitante_ms = 0
    tempo_regulador_ms = 0
    qtd_interacoes_regulacao = 0
    atores_envolvidos: set = set()

    data_inicio = None
    data_desfecho = None
    data_ultima_interacao = None
    posse_atual = "SOLICITANTE"  # O jogo sempre começa com o Solicitante

    # SLA V2: Defaults para novas colunas (Funil + Desfecho Qualitativo)
    data["SLA_Tipo_Desfecho"] = ""  # pragma: no mutate
    data["SLA_Marco_Autorizada"] = False  # pragma: no mutate
    data["SLA_Marco_Agendada"] = False  # pragma: no mutate
    data["SLA_Marco_Realizada"] = False  # pragma: no mutate

    # Computed fields
    data["dataCadastro"] = ""  # pragma: no mutate
    data["medicoSolicitante"] = ""  # pragma: no mutate

    # Dual-Write accumulators
    evolucoes_parsed = []
    textos_clinicos = []
    textos_completos = []
    first_clinical_found = False

    for evo in evolucoes_ordenadas:
        ts_atual = int(evo.get("data") or 0)
        dt_evo = timestamp_to_date(evo.get("data"))
        u_evo = evo.get("usuario") or {}
        usuario_nome = u_evo.get("nome", "Sistema")
        usuario_cpf = u_evo.get("cpf", "")
        perfil = evo.get("perfil") or ""
        oper = evo.get("operacaoSolicitacao", "")
        sit_atual = evo.get("situacaoAtual", "")
        sit_ant = evo.get("situacaoAnterior", "")

        det_json = _parse_detalhes(evo.get("detalhes", "{}"))

        # ── A. SNAPSHOT EXTRACTION (1ª Evolução com entidade) ────────────
        if data_inicio is None:
            data_inicio = ts_atual
            data_ultima_interacao = ts_atual

            entidade = det_json.get("entidade") or {}
            
            # pragma: no mutate
            if entidade:
                data["entidade_sistemaOrigem"] = entidade.get("sistemaOrigem", "")  # pragma: no mutate
                data["entidade_complexidade"] = entidade.get("complexidade", "")  # pragma: no mutate
                data["entidade_semClassificacao"] = safe_bool(  # pragma: no mutate
                    entidade.get("semClassificacao")
                )

                # pragma: no mutate
                cid = entidade.get("cidPrincipal") or {}
                data["entidade_cidPrincipal_codigo"] = cid.get("codigo", "")  # pragma: no mutate
                data["entidade_cidPrincipal_descricao"] = cid.get("descricao", "")  # pragma: no mutate

                esp = entidade.get("especialidade") or {}
                data["entidade_especialidade_descricao"] = esp.get("descricao", "")  # pragma: no mutate
                data["entidade_especialidade_descricaoAuxiliar"] = esp.get(  # pragma: no mutate
                    "descricaoAuxiliar", ""
                )
                data["entidade_especialidade_cbo_descricao"] = (  # pragma: no mutate
                    esp.get("cbo") or {}
                ).get("descricao", "")

                esp_mae = esp.get("especialidadeMae") or {}
                data["entidade_especialidade_especialidadeMae_descricao"] = esp_mae.get(  # pragma: no mutate
                    "descricao", ""
                )
                data["entidade_especialidade_especialidadeMae_cbo_descricao"] = (  # pragma: no mutate
                    esp_mae.get("cbo") or {}
                ).get("descricao", "")

                data["entidade_especialidade_tipoRegulacao"] = esp.get(  # pragma: no mutate
                    "tipoRegulacao", ""
                )
                data["entidade_especialidade_teleconsulta"] = safe_bool(  # pragma: no mutate
                    esp.get("teleconsulta")
                )
                data["entidade_especialidade_ativa"] = safe_bool(esp.get("ativa"))  # pragma: no mutate
                data["entidade_especialidade_matriciamento"] = safe_bool(  # pragma: no mutate
                    esp.get("matriciamento")
                )
                data["entidade_especialidade_tipoOCI"] = safe_bool(esp.get("tipoOCI"))  # pragma: no mutate

                # pragma: no mutate
                risco = entidade.get("classificacaoRisco") or {}
                data["entidade_classificacaoRisco_totalPontos"] = risco.get(  # pragma: no mutate
                    "totalPontos", 0
                )
                data["entidade_classificacaoRisco_pontosGravidade"] = risco.get(  # pragma: no mutate
                    "pontosGravidade", 0
                )
                data["entidade_classificacaoRisco_pontosTempo"] = risco.get(  # pragma: no mutate
                    "pontosTempo", 0
                )
                data["entidade_classificacaoRisco_cor"] = risco.get("cor", "")  # pragma: no mutate
                data["entidade_classificacaoRisco_reclassificadaSolicitante"] = (  # pragma: no mutate
                    safe_bool(risco.get("reclassificadaSolicitante"))
                )

                data["entidade_foraDaRegionalizacao"] = safe_bool(  # pragma: no mutate
                    entidade.get("foraDaRegionalizacao")
                )
                data["entidade_possuiDita"] = safe_bool(entidade.get("possuiDita"))  # pragma: no mutate

                # Geografias do Snapshot
                mun_sus_ent = entidade.get("municipioUsuarioSUS") or {}
                data["entidade_municipioUsuarioSUS_nome"] = mun_sus_ent.get("nome", "")  # pragma: no mutate
                data["entidade_municipioUsuarioSUS_uf"] = mun_sus_ent.get("uf", "")  # pragma: no mutate

                extract_unidade(
                    entidade.get("unidadeOperador"), "entidade_unidadeOperador", data
                )
                extract_unidade(
                    (entidade.get("unidadeOperador") or {}).get("centralRegulacao"),
                    "entidade_unidadeOperador_centralRegulacao",
                    data,
                )
                extract_unidade(
                    entidade.get("unidadeReferencia"),
                    "entidade_unidadeReferencia",
                    data,
                )
                extract_unidade(
                    (entidade.get("unidadeReferencia") or {}).get("centralRegulacao"),
                    "entidade_unidadeReferencia_centralRegulacao",
                    data,
                )
                extract_unidade(
                    entidade.get("centralRegulacao"), "entidade_centralRegulacao", data
                )

        # ── B. THE PING-PONG ENGINE (STATE MACHINE V2) ────────────────────

        # Definição dos Estados de Posse de Bola (Sets O(1) lookup)
        ESTADOS_PING_REGULADOR = {
            "AGUARDA_REGULACAO",
            "AGUARDA_REAVALIACAO",
            "AGUARDA_REVERSAO",
            "ENCAMINHADA_AO_NIR",
        }
        ESTADOS_PONG_SOLICITANTE = {
            "AUTORIZADA",
            "AGENDADA",
            "PENDENTE",
            "AGUARDA_MATRICIAMENTO",
            "AUTORIZACAO_AUTOMATICA",
            "CONFIRMACAO_EXPIRADA",
            "AGENDA_CONFIRMADA",
        }

        # Definição dos Estados de Desfecho
        ESTADOS_FIM_POSITIVO = {"REALIZADA"}
        ESTADOS_FIM_NEGATIVO = {"CANCELADA_SEM_REVERSAO", "CANCELADA"}
        ESTADOS_FIM_ABANDONO = {"ENCERRADA"}

        # 1. Adiciona o tempo decorrido ao "Dono da Posse" ANTES de trocar o estado
        if data_ultima_interacao is not None:
            delta = ts_atual - data_ultima_interacao
            if posse_atual == "SOLICITANTE":
                tempo_solicitante_ms += delta
            elif posse_atual == "REGULADOR":
                tempo_regulador_ms += delta

        data_ultima_interacao = ts_atual

        # 2. Rastreamento de Atores
        usuario = evo.get("usuario") or {}
        if usuario.get("nome"):
            atores_envolvidos.add(usuario.get("nome"))

        # 3. Troca de Posse baseada no Novo Estado (Determinístico)
        if sit_atual in ESTADOS_PING_REGULADOR:
            posse_atual = "REGULADOR"
            qtd_interacoes_regulacao += 1
        elif sit_atual in ESTADOS_PONG_SOLICITANTE:
            posse_atual = "SOLICITANTE"

        # 4. Rastreamento de Funil (Marcos da Jornada Feliz)
        if sit_atual == "AUTORIZADA":
            data["SLA_Marco_Autorizada"] = True  # pragma: no mutate
        elif sit_atual == "AGENDADA":
            data["SLA_Marco_Agendada"] = True  # pragma: no mutate

        # 5. Fim de Jogo (Desfecho)
        if (
            sit_atual in ESTADOS_FIM_POSITIVO
            or sit_atual in ESTADOS_FIM_NEGATIVO
            or sit_atual in ESTADOS_FIM_ABANDONO
        ):
            if data_desfecho is None:
                data_desfecho = ts_atual
                posse_atual = "FIM"  # Congela o cronômetro

                # Registra o tipo qualitativo do desfecho
                if sit_atual in ESTADOS_FIM_POSITIVO:
                    data["SLA_Tipo_Desfecho"] = "POSITIVO"  # pragma: no mutate
                    data["SLA_Marco_Realizada"] = True  # pragma: no mutate
                elif sit_atual in ESTADOS_FIM_NEGATIVO:
                    data["SLA_Tipo_Desfecho"] = "NEGATIVO"  # pragma: no mutate
                else:
                    data["SLA_Tipo_Desfecho"] = "ABANDONO"  # pragma: no mutate

        # ── C. DUAL-WRITE (Evolução Textual + JSON Analítico) ───────────
        itens = det_json.get("itensEvolucao", [])

        evo_dict = {
            "data": dt_evo,
            "usuario_nome": usuario_nome,
            "usuario_cpf": usuario_cpf,
            "perfil": perfil,
            "operacaoSolicitacao": oper,
            "situacaoAnterior": sit_ant,
            "situacaoAtual": sit_atual,
            "detalhes_limpos": [],
        }

        bloco_txt_completo = (
            f"[{dt_evo} | {usuario_nome}] -> {oper} ({sit_ant} ➔ {sit_atual})"
        )
        tem_detalhe_clinico = False

        for item in itens:
            label = item.get("label", item.get("codigo", "Informação"))
            texto = str(item.get("texto", "")).strip()
            if texto:
                evo_dict["detalhes_limpos"].append({"label": label, "texto": texto})
                bloco_txt_completo += f" | {label}: {texto}"

                label_norm = remove_accents(label.lower())
                if label_norm in [
                    "evolucao",
                    "parecer",
                    "comentarios",
                    "descricao do quadro clinico",
                    "anamnese",
                    "justificativa",
                ]:
                    textos_clinicos.append(f"[{dt_evo} | {usuario_nome}]: {texto}")
                    tem_detalhe_clinico = True

        evolucoes_parsed.append(evo_dict)
        textos_completos.append(bloco_txt_completo)

        # Primeiro evento clínico → dataCadastro + medicoSolicitante
        if tem_detalhe_clinico and not first_clinical_found:
            data["dataCadastro"] = dt_evo  # pragma: no mutate
            data["medicoSolicitante"] = usuario_nome  # pragma: no mutate
            first_clinical_found = True

        # ── D. DESFECHO: Break loop se atingiu fim de jogo ──────────────
        if data_desfecho is not None:
            break

    # ── E. ACTIVE TICKER (Processo ainda em andamento) ──────────────────
    if data_desfecho is None and data_ultima_interacao is not None:
        agora_ms = int(datetime.now().timestamp() * 1000)
        delta_agora = agora_ms - data_ultima_interacao
        if posse_atual == "SOLICITANTE":
            tempo_solicitante_ms += delta_agora
        else:
            tempo_regulador_ms += delta_agora

    # ── F. MÉTRICAS SLA CONSOLIDADAS ────────────────────────────────────
    MS_PER_DAY = 86400000.0
    data["SLA_Tempo_Solicitante_Dias"] = round(tempo_solicitante_ms / MS_PER_DAY, 2)  # pragma: no mutate
    data["SLA_Tempo_Regulador_Dias"] = round(tempo_regulador_ms / MS_PER_DAY, 2)  # pragma: no mutate
    data["SLA_Lead_Time_Total_Dias"] = round(  # pragma: no mutate
        (tempo_solicitante_ms + tempo_regulador_ms) / MS_PER_DAY, 2
    )
    data["SLA_Interacoes_Regulacao"] = qtd_interacoes_regulacao  # pragma: no mutate
    data["SLA_Atores_Envolvidos"] = " | ".join(sorted(atores_envolvidos))  # pragma: no mutate
    data["SLA_Desfecho_Atingido"] = bool(data_desfecho)  # pragma: no mutate

    # ── G. DUAL-WRITE FINAL ─────────────────────────────────────────────
    data["historico_quadro_clinico"] = " || ".join(textos_clinicos)  # pragma: no mutate
    data["historico_evolucoes_completo"] = " || ".join(textos_completos)  # pragma: no mutate
    data["evolucoes_json"] = json.dumps(evolucoes_parsed, ensure_ascii=False)  # pragma: no mutate

    return data
