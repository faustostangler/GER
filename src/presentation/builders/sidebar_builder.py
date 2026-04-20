import streamlit as st
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator
from presentation.components.filters import render_include_exclude, render_boolean_radio, render_presence_radio, render_dual_slider, render_age_slider, render_smart_date_range, render_advanced_text_search, render_outcome_type_filter, render_pending_reasons_filter

def build_sidebar(use_case, builder, st_user):
    """
    Constrói a barra lateral de filtros em cascata e retorna os estados visuais.
    """
    ui_filters = {
        "🩺 Clinical & Regulation": [],
        "🏛️ Governance & Actors": [],
        "📅 Lifecycle (Dates)": [],
        "🌍 Demographics & Network": [],
        "⚠️ Triage & Risk Classification": [],
        "🎯 Outcomes, Bottlenecks & SLA": [],
    }
    state_keys = {k: [] for k in ui_filters.keys()}
    curr_where = "1=1"

    # ==========================================
    # CASCADING SIDEBAR (OPTIMIZED TOP-DOWN FLOW)
    # ==========================================
    st.sidebar.header("🎛️ Cascading Filters")

    cat = "🩺 Clinical & Regulation"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(use_case, 
            "Parent Specialty",
            "entidade_especialidade_especialidadeMae_descricao", builder,
            curr_where,
            "espm",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Fine Specialty",
            "entidade_especialidade_descricao", builder,
            curr_where,
            "espf",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "CBO Specialty",
            "entidade_especialidade_cbo_descricao", builder,
            curr_where,
            "esp_cbo",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Auxiliary Description",
            "entidade_especialidade_descricaoAuxiliar", builder,
            curr_where,
            "esp_aux",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Requesting Doctor",
            "medicoSolicitante", builder,
            curr_where,
            "med_sol",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Operating Unit",
            "entidade_unidadeOperador_nome", builder,
            curr_where,
            "usol",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Main ICD (Code)",
            "entidade_cidPrincipal_codigo", builder,
            curr_where,
            "cid_cod",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_advanced_text_search(
            "Main ICD (Description)",
            "entidade_cidPrincipal_descricao", builder,
            "txt_cid_desc",
            ui_filters[cat],
            state_keys[cat],
        )
        # MOVED CLINICAL MAGIC: Aggregation by integer numeroCMCE
        st.markdown("---")
        curr_where = render_advanced_text_search(
            "Patient Evolutions",
            "historico_quadro_clinico", builder,
            "txt_evo",
            ui_filters[cat],
            state_keys[cat],
            aggregate_by="numeroCMCE",
        )
        curr_where = DuckDBCriteriaTranslator.translate(builder.build())

    cat = "🏛️ Governance & Actors"
    with st.sidebar.expander(cat, expanded=False):
        # Actors moved from the old Evolutions tab
        curr_where = render_advanced_text_search(
            "Information Type",
            "historico_evolucoes_completo", builder,
            "txt_tinf",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_advanced_text_search(
            "Information Source",
            "evolucoes_json", builder,
            "txt_orig_inf",
            ui_filters[cat],
            state_keys[cat],
        )
        st.markdown("---")

        curr_where = render_include_exclude(use_case, 
            "Source (List)",
            "origem_lista", builder,
            curr_where,
            "lst",
            ui_filters[cat],
            state_keys[cat],
            st_user,
            default_in=["Fila de Espera"],
        )
        curr_where = render_include_exclude(use_case, 
            "Current Situation",
            "situacao", builder,
            curr_where,
            "sit",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Regulation Type",
            "entidade_especialidade_tipoRegulacao", builder,
            curr_where,
            "treg",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Active Specialty",
            "entidade_especialidade_ativa", builder,
            curr_where,
            "stesp",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        curr_where = render_presence_radio(
            "Injunction / Court Order",
            "liminarOrdemJudicial", builder,
            "oj",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Operator",
            "operador_nome", builder,
            curr_where,
            "op_nome",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Requesting User",
            "usuarioSolicitante_nome", builder,
            curr_where,
            "usu_sol_nome",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Regulation Center",
            "entidade_centralRegulacao_nome", builder,
            curr_where,
            "cent_reg",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Operating Unit Reg. Center",
            "entidade_unidadeOperador_centralRegulacao_nome", builder,
            curr_where,
            "uni_op_cent",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Reference Unit",
            "entidade_unidadeReferencia_nome", builder,
            curr_where,
            "uni_ref",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Has DITA",
            "entidade_possuiDita", builder,
            "dita",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Outside Regionalization",
            "entidade_foraDaRegionalizacao", builder,
            "freg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Access Regularization",
            "regularizacaoAcesso", builder,
            "reg_acc",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Accepts Teleconsultation",
            "entidade_especialidade_teleconsulta", builder,
            "tele",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Matrix Support",
            "entidade_especialidade_matriciamento", builder,
            "matri",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Unclassified",
            "entidade_semClassificacao", builder,
            "sem_class",
            ui_filters[cat],
            state_keys[cat],
        )

        curr_where = DuckDBCriteriaTranslator.translate(builder.build())

    cat = "📅 Lifecycle (Dates)"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_smart_date_range(
            "Request Date",
            "dataSolicitacao", builder,
            "dt_solic",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Registration Date",
            "dataCadastro", builder,
            "dt_cad",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "Evolution Date",
            "dataCadastro", builder,
            "dt_evo",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "First Appointment",
            "dataPrimeiroAgendamento", builder,
            "dt_pagend",
            ui_filters[cat],
            state_keys[cat],
        )
        st.write(" ")
        curr_where = render_smart_date_range(
            "First Authorization",
            "dataPrimeiraAutorizacao", builder,
            "dt_paut",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🌍 Demographics & Network"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_advanced_text_search(
            "Search: Patient Name",
            "usuarioSUS_nomeCompleto", builder,
            "txt_pac_nome",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = DuckDBCriteriaTranslator.translate(builder.build())
        st.markdown("---")

        curr_where = render_include_exclude(use_case, 
            "Municipality of Residence",
            "usuarioSUS_municipioResidencia_nome", builder,
            curr_where,
            "mun",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Neighborhood",
            "usuarioSUS_bairro", builder,
            curr_where,
            "bai",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        # Logradouro with conditional injecting numbering inside Deep Search
        curr_where = render_advanced_text_search(
            "Street",
            "usuarioSUS_logradouro", builder,
            "txt_logr",
            ui_filters[cat],
            state_keys[cat],
        )
        if st.session_state.get("txt_logr_toggle", False):
            st.markdown(
                "<div class='sre-filter-group'>",
                unsafe_allow_html=True,
            )
            state_keys[cat].extend(["num_min", "num_max"])
            # SRE FIX: Initializes state before widget to avoid value mismatch
            if "num_min" not in st.session_state:
                st.session_state["num_min"] = 0
            if "num_max" not in st.session_state:
                st.session_state["num_max"] = 99999
            col_nmin, col_nmax = st.columns(2)
            v_nmin = col_nmin.number_input(
                "Min No.",
                min_value=0,
                max_value=99999,
                step=10,
                key="num_min",
                label_visibility="collapsed",
            )
            v_nmax = col_nmax.number_input(
                "Max No.",
                min_value=0,
                max_value=99999,
                step=100,
                key="num_max",
                label_visibility="collapsed",
            )
            if v_nmin > 0 or v_nmax < 99999:
                ui_filters[cat].append(
                    {
                        "text": f"Street Number: {v_nmin} to {v_nmax}",
                        "keys": ["num_min", "num_max"],
                    }
                )
                builder.add_limite_numerico(
                    "usuarioSUS_numero", v_nmin, v_nmax
                )
            st.markdown("</div>", unsafe_allow_html=True)

        st.divider()  # --- Visual Separator for Personal Identification ---

        curr_where = DuckDBCriteriaTranslator.translate(builder.build())
        curr_where = render_include_exclude(use_case, 
            "Sex",
            "usuarioSUS_sexo", builder,
            curr_where,
            "sex",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        # Component that injects entidade_idade_idadeInteiro (with Dual Slider)
        curr_where = render_age_slider(use_case, 
            "Age Group (Age)", builder, "f_idade", ui_filters[cat], state_keys[cat]
        )

        curr_where = render_include_exclude(use_case, 
            "Race/Color",
            "usuarioSUS_racaCor", builder,
            curr_where,
            "cor",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Nationality",
            "usuarioSUS_nacionalidade", builder,
            curr_where,
            "nac",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

    cat = "⚠️ Triage & Risk Classification"
    with st.sidebar.expander(cat, expanded=False):
        curr_where = render_include_exclude(use_case, 
            "Complexity",
            "entidade_complexidade", builder,
            curr_where,
            "cpx",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Risk Color (Current)",
            "entidade_classificacaoRisco_cor", builder,
            curr_where,
            "r_cor",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Regulator Color",
            "corRegulador", builder,
            curr_where,
            "c_reg",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        curr_where = render_boolean_radio(
            "Reclassified by Requester",
            "entidade_classificacaoRisco_reclassificadaSolicitante", builder,
            "r_recl",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        curr_where = render_dual_slider(use_case, 
            "Gravity Points",
            "entidade_classificacaoRisco_pontosGravidade", builder,
            "pt_grav",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time Points",
            "entidade_classificacaoRisco_pontosTempo", builder,
            "pt_tmp",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Total Points",
            "entidade_classificacaoRisco_totalPontos", builder,
            "pt_tot",
            ui_filters[cat],
            state_keys[cat],
        )

    cat = "🎯 Outcomes, Bottlenecks & SLA"
    with st.sidebar.expander(cat, expanded=False):
        # 1. Outcome Type — includes "IN PROGRESS" for cases without outcome yet
        curr_where = render_outcome_type_filter(
            use_case,
            "Outcome Type",
            "SLA_Tipo_Desfecho",
            builder,
            curr_where,
            "sla_tipo",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        curr_where = render_include_exclude(use_case, 
            "Provisional Status",
            "statusProvisorio", builder,
            curr_where,
            "st_prov",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        # Pending Reason — extracts 4 fields from JSON via DuckDB json_extract_string
        # WHY: get_dynamic_options("{expr}") wraps the argument with double quotes,
        # making the SQL expression invalid. The query is made directly in the use_case.
        curr_where = render_pending_reasons_filter(
            use_case,
            "📦 Pending Reason",
            builder,
            curr_where,
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        curr_where = render_include_exclude(use_case, 
            "Cancellation Reason",
            "motivoCancelamento", builder,
            curr_where,
            "mot_canc",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )
        curr_where = render_include_exclude(use_case, 
            "Closure Reason",
            "motivoEncerramento", builder,
            curr_where,
            "mot_enc",
            ui_filters[cat],
            state_keys[cat],
            st_user,
        )

        st.markdown("---")
        # 2. Textos de Justificativa (Deep Search)  (Keep comments largely in English if desired, but focus on the UI strings)
        curr_where = render_advanced_text_search(
            "Return Justification",
            "justificativaRetorno", builder,
            "txt_retorno",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 3. Marcos de Sucesso (Booleans)
        curr_where = render_boolean_radio(
            "1. Was Authorized?",
            "SLA_Marco_Autorizada", builder,
            "m_aut",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "2. Was Scheduled?",
            "SLA_Marco_Agendada", builder,
            "m_agd",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "3. Was Accomplished?",
            "SLA_Marco_Realizada", builder,
            "m_rea",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_boolean_radio(
            "Queue Finished? (Timer Stopped)",
            "SLA_Desfecho_Atingido", builder,
            "m_fim",
            ui_filters[cat],
            state_keys[cat],
        )

        st.markdown("---")
        # 4. Sliders de SLA (Métricas calculadas em dias e interações)
        curr_where = render_dual_slider(use_case, 
            "Total Lead Time (Days)",
            "SLA_Lead_Time_Total_Dias", builder,
            "sla_tot",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time with Regulator (Days)",
            "SLA_Tempo_Regulador_Dias", builder,
            "sla_reg",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Time with Requester (Days)",
            "SLA_Tempo_Solicitante_Dias", builder,
            "sla_sol",
            ui_filters[cat],
            state_keys[cat],
        )
        curr_where = render_dual_slider(use_case, 
            "Interaction Volume (Ping-Pong)",
            "SLA_Interacoes_Regulacao", builder,
            "sla_int",
            ui_filters[cat],
            state_keys[cat],
        )

    return ui_filters, state_keys, curr_where
