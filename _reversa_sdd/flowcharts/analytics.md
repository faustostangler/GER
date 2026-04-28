# Fluxos de Execução - Módulo Analytics

Este documento contém diagramas comportamentais do fluxo de dados e controle analítico.

## 1. Fluxo de Resolução de Consulta Analítica

A sequência abaixo ilustra o desacoplamento do domínio via Arquitetura Hexagonal durante o carregamento do dashboard.

```mermaid
sequenceDiagram
    participant UI as Streamlit (app_analytics.py)
    participant UC as AnalyticsUseCase
    participant Spec as FiltroAvancadoSpec
    participant Rep as DuckDBAnalyticsRepository
    participant Trans as DuckDBCriteriaTranslator
    participant DB as DuckDB OLAP

    UI->>Spec: Compila filtros ativos na tela
    UI->>UC: Solicita get_executive_summary(spec)
    UC->>Rep: Dispara consulta repassando a spec
    Rep->>Trans: translate(spec)
    Trans-->>Rep: Retorna Predicados SQL ("dias_fila > 30")
    Rep->>DB: Executa SELECT + RLS + Predicados
    DB-->>Rep: Retorna Pandas DataFrame
    Rep-->>UC: Converte em AnalyticKPIs (Domínio)
    UC-->>UI: Renderiza componentes visuais (KPI Boards)
```

## 2. Decisão de Cache Distribuído e RLS

Fluxo lógico do Adapter `DuckDBAnalyticsRepository` ao interagir com segurança e performance.

```mermaid
graph TD
    Start([Recebe Query SQL + Token do Usuário]) --> RLS[Aplica _get_rls_cte baseada no Perfil IAM]
    RLS --> CacheCheck{Existe chave no Redis?}
    CacheCheck -- SIM --> PaReturn[Lê buffer PyArrow Feather] --> End([Retorna DataFrame])
    CacheCheck -- NÃO --> Execute[Dispara Query no DuckDB]
    Execute --> SaveCache[Serializa PyArrow IPC]
    SaveCache --> RedisSet[Persiste no Redis por 10m]
    RedisSet --> End
```
