from typing import Dict, Any
import json
from datetime import datetime

COLUNAS = [
    "Protocolo", "Situação", "Origem da Lista", "Data Solicitação", 
    "Nome do Paciente", "Data de Nascimento", "Sexo", "Cor", "CPF", 
    "Nome da Mãe", "Cartão SUS", "Logradouro", "Número", "Complemento", 
    "Bairro", "CEP", "Município de Residência", "Nacionalidade", "Ordem Judicial",
    "Especialidade Mãe", "Especialidade", "Especialidade Descrição Auxiliar", "Especialidade CBO",
    "Tipo de Regulação", "Teleconsulta", "Status da Especialidade",
    "Complexidade", "Risco Cor", "Cor Regulador", "Pontos Gravidade", "Pontos Tempo", "Pontuação", "Situação Final",
    "CID Código", "CID Descrição", "Unidade Solicitante",
    "Operador", "Usuário Solicitante", "Unidade Razão Social", "Unidade Descrição",
    "Central de Regulação", "Origem da Regulação",
    "Data do Cadastro", "Médico Solicitante",
    "Histórico Quadro Clínico"
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

def clean_data_row(data: Dict[str, Any]) -> Dict[str, Any]:
    cleaned_row = {}
    for col in COLUNAS:
        v = data.get(col, "")
        if isinstance(v, str):
            cleaned_row[col] = v.replace("\r\n", "\n").replace("\r", "\n")
        else:
            cleaned_row[col] = v
    return cleaned_row

def flatten_solicitacao(j: Dict[Any, Any], origem_lista: str) -> Dict[str, Any]:
    """
    Transforma o JSON complexo em uma linha plana para o CSV e modelagem visual.
    Lógica de negócio pura, 100% isolada e dependente do Domínio.
    """
    data = {}
    data["Protocolo"] = format_protocolo(j.get("numeroCMCE", ""))
    data["Situação"] = j.get("situacao", "")
    data["Origem da Lista"] = origem_lista
    data["Data Solicitação"] = timestamp_to_date(j.get("dataSolicitacao"))
    data["Ordem Judicial"] = j.get("liminarOrdemJudicial", "")
    
    u = j.get("usuarioSUS") or {}
    data["Nome do Paciente"] = u.get("nomeCompleto", "")
    data["Data de Nascimento"] = timestamp_to_date(u.get("dataNascimento")).split(" ")[0] if timestamp_to_date(u.get("dataNascimento")) else ""
    data["Sexo"] = u.get("sexo", "")
    data["Cor"] = u.get("racaCor", "")
    data["CPF"] = u.get("cpf", "")
    data["Nome da Mãe"] = u.get("nomeMae", "")
    data["Cartão SUS"] = u.get("cartaoSus", "")
    
    data["Logradouro"] = u.get("logradouro", "")
    data["Número"] = u.get("numero", "")
    data["Complemento"] = u.get("complemento", "")
    data["Bairro"] = u.get("bairro", "")
    data["CEP"] = u.get("cep", "")
    data["Município de Residência"] = (u.get("municipioResidencia") or {}).get("nome", "")
    data["Nacionalidade"] = u.get("nacionalidade", "")
    
    esp = j.get("especialidade") or {}
    esp_mae = esp.get("especialidadeMae") or {}
    data["Especialidade Mãe"] = esp_mae.get("descricao", "")
    data["Especialidade"] = esp.get("descricao", "")
    data["Especialidade Descrição Auxiliar"] = esp.get("descricaoAuxiliar", "")
    data["Especialidade CBO"] = (esp_mae.get("cbo") or {}).get("descricao", "")
    data["Tipo de Regulação"] = esp.get("tipoRegulacao", "")
    data["Teleconsulta"] = esp.get("teleconsulta", "")
    data["Status da Especialidade"] = esp.get("ativa", "")
    
    risk = j.get("classificacaoRisco") or {}
    data["Complexidade"] = j.get("complexidade", "")
    data["Risco Cor"] = risk.get("cor", "")
    data["Cor Regulador"] = j.get("corRegulador", "")
    data["Pontos Gravidade"] = risk.get("pontosGravidade", "")
    data["Pontos Tempo"] = risk.get("pontosTempo", "")
    data["Pontuação"] = risk.get("totalPontos", "")
    data["Situação Final"] = j.get("situacao", "")
    
    data["CID Código"] = (j.get("cidPrincipal") or {}).get("codigo", "")
    data["CID Descrição"] = (j.get("cidPrincipal") or {}).get("descricao", "")
    data["Unidade Solicitante"] = (j.get("unidadeSolicitante") or {}).get("nome", "")
    
    op = j.get("operador") or {}
    data["Operador"] = op.get("nome") or (op.get("profissional") or {}).get("nome", "")
    
    us = j.get("usuarioSolicitante") or {}
    data["Usuário Solicitante"] = us.get("nome") or (us.get("profissional") or {}).get("nome", "")
    
    uop = j.get("unidadeOperador") or {}
    data["Unidade Razão Social"] = uop.get("razaoSocial", "")
    data["Unidade Descrição"] = (uop.get("tipoUnidade") or {}).get("descricao", "")
    
    data["Central de Regulação"] = (j.get("centralRegulacao") or {}).get("nome", "")
    data["Origem da Regulação"] = (j.get("centralRegulacaoOrigem") or {}).get("nome", "")

    # Linha do tempo de evoluções
    evolucoes = j.get("evolucoes", [])
    evolucoes.sort(key=lambda x: x.get("data", 0))
    
    data["Data do Cadastro"] = ""
    data["Médico Solicitante"] = ""
    
    historico_textos = []
    first_evo_found = False
    
    for evo in evolucoes:
        dt_evo = timestamp_to_date(evo.get("data"))
        usuario = (evo.get("usuario") or {}).get("nome", "Sistema")
        
        try:
            detalhes_str = evo.get("detalhes", "{}")
            detalhes_json = json.loads(detalhes_str) if isinstance(detalhes_str, str) else detalhes_str
            
            itens = detalhes_json.get("itensEvolucao", [])
            has_valid_text = False
            for item in itens:
                label = item.get("label", item.get("codigo", "Informação"))
                texto = str(item.get("texto", "")).strip()
                if texto:
                    has_valid_text = True
                    linha_evo = f"\n\n[{dt_evo} | {label} | {usuario}]: {texto}"
                    historico_textos.append(linha_evo)
            
            if has_valid_text and not first_evo_found:
                data["Data do Cadastro"] = dt_evo
                data["Médico Solicitante"] = usuario
                first_evo_found = True
        except Exception:
            continue
            
    data["Histórico Quadro Clínico"] = " | ".join(historico_textos)
    
    return data
