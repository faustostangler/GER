from typing import Dict, Any
import json
from datetime import datetime

COLUNAS = [
    # Metadados e Identificação
    "Protocolo", "Situação", "Origem da Lista", "Data Solicitação", 
    "Data do Cadastro", "Médico Solicitante",
    
    # 1. Paciente & Demografia
    "Nome do Paciente", "Data de Nascimento", "Sexo", "Cor", "CPF", 
    "Nome da Mãe", "Cartão SUS", "Nacionalidade",
    "Município Paciente", "UF Paciente", "Telefones Paciente",
    "Logradouro", "Número", "Complemento", "Bairro", "CEP",
    
    # 2. Especialidade & Acesso
    "Especialidade Mãe", "Especialidade", "Especialidade Descrição Auxiliar", "Especialidade CBO",
    "Especialidade Tipo Regulação", "Teleconsulta", "Especialidade Ativa", 
    "Especialidade Matriciamento", "Especialidade OCI",
    "Regularização de Acesso", "Fora da Regionalização", "Possui DITA", "Ordem Judicial",
    
    # 3. Triagem & Risco
    "Complexidade", "Risco Cor", "Cor Regulador", "Pontos Gravidade", "Pontos Tempo", "Pontuação", 
    "Situação Final", "CID Código", "CID Descrição", "Triagem Reclassificada",
    
    # 4. Atores & Operadores (Governança)
    "Operador Nome", "Operador CPF", 
    "Usuário Solicitante Nome", "Usuário Solicitante CPF",
    
    # 5. Unidades de Saúde (Extensa Rastreabilidade)
    "Unidade Operador Nome", "Unidade Operador Tipo", "Unidade Operador Razão Social", "Unidade Operador Município", "Unidade Operador UF",
    "Unidade Operador Central Regulação Nome", "Unidade Operador Central Regulação Tipo",
    
    "Unidade Solicitante Nome", "Unidade Solicitante Tipo", "Unidade Solicitante Razão Social", "Unidade Solicitante Município", "Unidade Solicitante UF",
    "Unidade Solicitante Central Regulação Nome", "Unidade Solicitante Central Regulação Tipo",
    
    "Unidade Referência Nome", "Unidade Referência Tipo", "Unidade Referência Razão Social", "Unidade Referência Município", "Unidade Referência UF",
    "Unidade Referência Central Regulação Nome", "Unidade Referência Central Regulação Tipo",
    "Unidade Referência Central Regulação Razão Social", "Unidade Referência Central Regulação Município", "Unidade Referência Central Regulação UF",
    
    # Legado de compatibilidade na borda
    "Central de Regulação", "Origem da Regulação", "Unidade Solicitante", "Operador", "Usuário Solicitante", "Unidade Razão Social", "Unidade Descrição",
    
    # 6. Ciclo de Vida & Timeline
    "Data Primeiro Agendamento", "Data Primeira Autorização", "Status Provisório",
    "Justificativa Retorno", "Justificativa Duplicação", "Motivo Pendência",
    "Motivo Cancelamento", "Motivo Encerramento", "Descrição Encerramento",
    
    # 7. Evoluções (Dual-Write: Textual e Analítica)
    "Histórico Quadro Clínico", "Histórico de Evoluções Completo", "Evoluções Detalhadas JSON"
]

def format_protocolo(num: Any) -> str:
    if not num: return ""
    num = str(num)
    if len(num) == 12:
        return f"{num[0:2]}-{num[2:4]}-{num[4:11]}-{num[11]}"
    return num

def timestamp_to_date(ts: Any) -> str:
    if not ts: return ""
    try:
        dt = datetime.fromtimestamp(float(ts) / 1000.0)
        return dt.strftime("%d/%m/%Y %H:%M:%S")
    except:
        return ""

def safe_bool(val: Any) -> bool:
    """ SRE FIX: Garante inferência BOOLEAN nativa no DuckDB/Parquet e evita falhas de NoneType """
    if val is None: return False
    if isinstance(val, bool): return val
    if isinstance(val, str):
        return val.strip().lower() in ["true", "1", "yes", "sim", "s"]
    return bool(val)

def extract_municipio(mun_dict: Dict[Any, Any]) -> tuple:
    """ SRE FIX: Extração segura de município e UF evitando KeyError. Retorna (Nome, UF) """
    if not mun_dict or not isinstance(mun_dict, dict):
        return ("", "")
    return (mun_dict.get("nome", ""), mun_dict.get("uf", ""))

def clean_data_row(data: Dict[str, Any]) -> Dict[str, Any]:
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = v.replace("\r\n", " ").replace("\r", " ").replace("\n", "  ")
        else:
            cleaned_row[col] = v
    return cleaned_row

def flatten_solicitacao(j: Dict[Any, Any], origem_lista: str) -> Dict[str, Any]:
    """
    Transforma o JSON complexo em uma linha plana para o CSV e Parquet seguindo Ubiquitous Language.
    """
    data = {}
    # -- Identificadores Básicos --
    data["Protocolo"] = format_protocolo(j.get("numeroCMCE", ""))
    data["Situação"] = j.get("situacao", "")
    data["Situação Final"] = j.get("situacao", "")
    data["Origem da Lista"] = origem_lista
    data["Ordem Judicial"] = j.get("liminarOrdemJudicial", "")
    data["Data Solicitação"] = timestamp_to_date(j.get("dataSolicitacao"))
    
    # -- 1. Paciente & Demografia --
    u = j.get("usuarioSUS") or {}
    data["Nome do Paciente"] = u.get("nomeCompleto", "")
    data["Data de Nascimento"] = timestamp_to_date(u.get("dataNascimento")).split(" ")[0] if timestamp_to_date(u.get("dataNascimento")) else ""
    data["Sexo"] = u.get("sexo", "")
    data["Cor"] = u.get("racaCor", "")
    data["CPF"] = u.get("cpf", "")
    data["Nome da Mãe"] = u.get("nomeMae", "")
    data["Cartão SUS"] = u.get("cartaoSus", "")
    data["Nacionalidade"] = u.get("nacionalidade", "")
    
    data["Telefones Paciente"] = u.get("telefones", "")
    mun_pac_nome, mun_pac_uf = extract_municipio(u.get("municipioResidencia"))
    data["Município Paciente"] = mun_pac_nome
    data["UF Paciente"] = mun_pac_uf
    
    data["Logradouro"] = u.get("logradouro", "")
    data["Número"] = u.get("numero", "")
    data["Complemento"] = u.get("complemento", "")
    data["Bairro"] = u.get("bairro", "")
    data["CEP"] = u.get("cep", "")
    
    # -- 2. Especialidade & Acesso --
    esp = j.get("especialidade") or {}
    esp_mae = esp.get("especialidadeMae") or {}
    data["Especialidade Mãe"] = esp_mae.get("descricao", "")
    data["Especialidade"] = esp.get("descricao", "")
    data["Especialidade Descrição Auxiliar"] = esp.get("descricaoAuxiliar", "")
    data["Especialidade CBO"] = (esp_mae.get("cbo") or {}).get("descricao", "")
    
    data["Especialidade Tipo Regulação"] = esp.get("tipoRegulacao", "")
    data["Teleconsulta"] = esp.get("teleconsulta", "")
    data["Especialidade Ativa"] = safe_bool(esp.get("ativa"))
    data["Especialidade Matriciamento"] = safe_bool(esp.get("matriciamento"))
    data["Especialidade OCI"] = safe_bool(esp.get("tipoOCI"))
    
    data["Regularização de Acesso"] = j.get("regularizacaoAcesso", "")
    data["Fora da Regionalização"] = safe_bool(j.get("foraDaRegionalizacao"))
    data["Possui DITA"] = safe_bool(j.get("possuiDita"))
    
    # -- 3. Triagem & Risco --
    risk = j.get("classificacaoRisco") or {}
    data["Complexidade"] = j.get("complexidade", "")
    data["Risco Cor"] = risk.get("cor", "")
    data["Cor Regulador"] = j.get("corRegulador", "")
    data["Pontos Gravidade"] = risk.get("pontosGravidade", "")
    data["Pontos Tempo"] = risk.get("pontosTempo", "")
    data["Pontuação"] = risk.get("totalPontos", "")
    
    data["CID Código"] = (j.get("cidPrincipal") or {}).get("codigo", "")
    data["CID Descrição"] = (j.get("cidPrincipal") or {}).get("descricao", "")
    data["Triagem Reclassificada"] = safe_bool(risk.get("reclassificadaSolicitante"))
    
    # -- 4. Atores & Operadores (Governança) --
    op = j.get("operador") or {}
    op_prof = op.get("profissional") or {}
    data["Operador Nome"] = op.get("nome") or op_prof.get("nome", "")
    data["Operador CPF"] = op.get("cpf") or op_prof.get("cpf", "")
    
    us = j.get("usuarioSolicitante") or {}
    us_prof = us.get("profissional") or {}
    data["Usuário Solicitante Nome"] = us.get("nome") or us_prof.get("nome", "")
    data["Usuário Solicitante CPF"] = us.get("cpf") or us_prof.get("cpf", "")
    
    # Campos Legado
    data["Operador"] = data["Operador Nome"]
    data["Usuário Solicitante"] = data["Usuário Solicitante Nome"]
    
    # -- 5. Unidades de Saúde (Extensa Rastreabilidade) --
    def parse_unidade(un_obj: dict, prefix: str):
        if not un_obj: un_obj = {}
        data[f"{prefix} Nome"] = un_obj.get("nome", "")
        data[f"{prefix} Tipo"] = (un_obj.get("tipoUnidade") or {}).get("descricao", "")
        data[f"{prefix} Razão Social"] = un_obj.get("razaoSocial", "")
        m_nome, m_uf = extract_municipio(un_obj.get("municipio"))
        data[f"{prefix} Município"] = m_nome
        data[f"{prefix} UF"] = m_uf
        
        c_reg = un_obj.get("centralRegulacao") or {}
        data[f"{prefix} Central Regulação Nome"] = c_reg.get("nome", "")
        data[f"{prefix} Central Regulação Tipo"] = (c_reg.get("tipoUnidade") or {}).get("descricao", "")
        return c_reg
        
    parse_unidade(j.get("unidadeOperador"), "Unidade Operador")
    u_sol_cent = parse_unidade(j.get("unidadeSolicitante"), "Unidade Solicitante")
    u_ref_cent = parse_unidade(j.get("unidadeReferencia"), "Unidade Referência")
    
    # Campos Extras para Unidade Referência Central de Regulação
    data["Unidade Referência Central Regulação Razão Social"] = u_ref_cent.get("razaoSocial", "")
    u_ref_cent_mun_nome, u_ref_cent_mun_uf = extract_municipio(u_ref_cent.get("municipio"))
    data["Unidade Referência Central Regulação Município"] = u_ref_cent_mun_nome
    data["Unidade Referência Central Regulação UF"] = u_ref_cent_mun_uf
    
    # Legado de compatibilidade
    data["Unidade Solicitante"] = data.get("Unidade Solicitante Nome", "")
    data["Unidade Descrição"] = data.get("Unidade Operador Tipo", "")
    data["Unidade Razão Social"] = data.get("Unidade Operador Razão Social", "")
    data["Central de Regulação"] = data.get("Unidade Operador Central Regulação Nome", "")
    data["Origem da Regulação"] = (j.get("centralRegulacaoOrigem") or {}).get("nome", "")
    
    # -- 6. Ciclo de Vida & Timeline --
    data["Data Primeiro Agendamento"] = timestamp_to_date(j.get("dataPrimeiroAgendamento"))
    data["Data Primeira Autorização"] = timestamp_to_date(j.get("dataPrimeiraAutorizacao"))
    data["Status Provisório"] = j.get("statusProvisorio", "")
    data["Justificativa Retorno"] = j.get("justificativaRetorno", "")
    data["Justificativa Duplicação"] = j.get("justificativaDuplicacao", "")
    data["Motivo Pendência"] = j.get("motivoPendencia", "")
    data["Motivo Cancelamento"] = j.get("motivoCancelamento", "")
    data["Motivo Encerramento"] = j.get("motivoEncerramento", "")
    data["Descrição Encerramento"] = j.get("descricaoEncerramento", "")
    
    # -- 7. Evoluções (SRE Dual-Write Strategy) --
    evolucoes_json_raw = j.get("evolucoes", [])
    evolucoes_json_raw.sort(key=lambda x: x.get("data", 0))
    
    data["Data do Cadastro"] = ""
    data["Médico Solicitante"] = ""
    
    evolucoes_parsed = []
    textos_clinicos = []       # Apenas laudos/evoluções clínicas limpas
    textos_completos = []      # Timeline estruturada para a UI
    
    first_evo_found = False
    
    for evo in evolucoes_json_raw:
        dt_evo = timestamp_to_date(evo.get("data"))
        u_evo = evo.get("usuario") or {}
        usuario_nome = u_evo.get("nome", "Sistema")
        usuario_cpf = u_evo.get("cpf", "")
        
        sit_atual = evo.get("situacaoAtual", "")
        sit_ant = evo.get("situacaoAnterior", "")
        oper = evo.get("operacaoSolicitacao", "")
        perfil = evo.get("perfil", "")
        
        # SRE FIX: Parsing defensivo do JSON 'detalhes'
        detalhes_str = evo.get("detalhes", "{}")
        detalhes_json = {}
        try:
            if isinstance(detalhes_str, str) and detalhes_str.strip().startswith("{"):
                detalhes_json = json.loads(detalhes_str)
            elif isinstance(detalhes_str, dict):
                detalhes_json = detalhes_str
        except Exception:
            pass # Failsafe
            
        itens = detalhes_json.get("itensEvolucao", [])
        
        # 1. Pipeline Analítico Estruturado (Prepara o dict para o DuckDB)
        evo_dict = {
            "data": dt_evo,
            "usuario_nome": usuario_nome,
            "usuario_cpf": usuario_cpf,
            "perfil": perfil,
            "operacaoSolicitacao": oper,
            "situacaoAnterior": sit_ant,
            "situacaoAtual": sit_atual,
            "detalhes_limpos": []
        }
        
        # Formatação do bloco legível para a Timeline (Meso)
        bloco_txt_completo = f"[{dt_evo} | {usuario_nome}] -> {oper} ({sit_ant} ➔ {sit_atual})"
        tem_detalhe_clinico = False
        
        for item in itens:
            label = item.get("label", item.get("codigo", "Informação"))
            texto = str(item.get("texto", "")).strip()
            if texto:
                evo_dict["detalhes_limpos"].append({"label": label, "texto": texto})
                bloco_txt_completo += f" | {label}: {texto}"
                
                # Se for evolução (parecer), anexa no Micro Clínico
                if label.lower() in ['evolução', 'parecer', 'comentários']:
                    textos_clinicos.append(f"[{dt_evo} | {usuario_nome}]: {texto}")
                    tem_detalhe_clinico = True

        evolucoes_parsed.append(evo_dict)
        textos_completos.append(bloco_txt_completo)
        
        # Marca o primeiro evento válido como Data de Cadastro Original
        if tem_detalhe_clinico and not first_evo_found:
            data["Data do Cadastro"] = dt_evo
            data["Médico Solicitante"] = usuario_nome
            first_evo_found = True

    # Popula as colunas Dual-Write (String Textual + String JSON)
    data["Histórico Quadro Clínico"] = " || ".join(textos_clinicos)
    data["Histórico de Evoluções Completo"] = " || ".join(textos_completos)
    data["Evoluções Detalhadas JSON"] = json.dumps(evolucoes_parsed, ensure_ascii=False)
    
    return data
