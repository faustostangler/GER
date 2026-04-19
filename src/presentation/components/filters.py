import streamlit as st
import pandas as pd
from datetime import date, timedelta
from domain.specifications import FiltroAvancadoSpecBuilder
from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator


def clear_filter_state(keys_to_clear: list):
    """
    Clears the filter state in Streamlit's session_state.
    WHY: Deep Search text_inputs use two pairs of keys:
      - `{key}_or_val` / `{key}_and_val` / `{key}_not_val` → backing store (logical value)
      - `{key}_or`     / `{key}_and`     / `{key}_not`     → Streamlit widget key

    Both must be cleared simultaneously so the sidebar reflects the clearing.
    If only _val is cleared, the Streamlit widget keeps the old text on the next render.
    If only the widget key is deleted, the next render creates a new empty widget
    but _val is still filled, causing phantom re-filtering.
    """
    for key in keys_to_clear:
        if key in st.session_state:
            if key.endswith("_in") or key.endswith("_ex"):
                st.session_state[key] = []
            elif key.endswith("_val"):
                # SRE FIX: Clears both backing store AND corresponding widget key (without _val suffix)
                st.session_state[key] = ""
                widget_key = key[:-4]  # Remove "_val" → get widget key
                if widget_key in st.session_state:
                    st.session_state[widget_key] = ""
            elif key.endswith("_toggle"):
                st.session_state[key] = False
            elif key.endswith("_or") or key.endswith("_and") or key.endswith("_not"):
                # Widget key direct from text_input — clears visible text in sidebar
                st.session_state[key] = ""
                # Also clears the corresponding backing store _val (mirror)
                val_key = f"{key}_val"
                if val_key in st.session_state:
                    st.session_state[val_key] = ""
            elif key == "num_min":
                st.session_state[key] = 0
            elif key == "num_max":
                st.session_state[key] = 99999
            elif key == "oj_radio":
                st.session_state[key] = "Both"
            else:
                try:
                    del st.session_state[key]
                except Exception:
                    pass


def render_include_exclude(
    use_case,
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    current_where: str,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    current_user,
    default_in: list = None,
):
    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    options = use_case.get_dynamic_options(column, current_where, current_user)
    if not options:
        return current_where

    # SRE FIX: Default state injection only on cold boot
    if f"{key}_in" not in st.session_state:
        if default_in:
            st.session_state[f"{key}_in"] = [v for v in default_in if v in options]
        else:
            st.session_state[f"{key}_in"] = []

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    incl = st.multiselect(
        "✅ Include",
        options,
        key=f"{key}_in",
        label_visibility="collapsed",
        placeholder="✅ Include...",
    )
    excl = st.multiselect(
        "❌ Exclude",
        options,
        key=f"{key}_ex",
        label_visibility="collapsed",
        placeholder="❌ Exclude...",
    )

    if incl:
        ui_tracker.append(
            {
                "text": f"✅ {label}: {', '.join([str(v) for v in incl])}",
                "keys": [f"{key}_in"],
            }
        )
        sanitized_incl = [str(v) for v in incl]
        builder.add_inclusao(column, sanitized_incl)

    if excl:
        ui_tracker.append(
            {
                "text": f"❌ {label}: {', '.join([str(v) for v in excl])}",
                "keys": [f"{key}_ex"],
            }
        )
        sanitized_excl = [str(v) for v in excl]
        builder.add_exclusao(column, sanitized_excl)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_boolean_radio(
    label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE Component for boolean fields (True/False/Null)"""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Both"

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Both", "Yes", "No"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Yes":
        ui_tracker.append({"text": f"{label}: Yes", "keys": [f"{key}_radio"]})
        builder.add_booleano(column, True)
    elif val == "No":
        ui_tracker.append({"text": f"{label}: No", "keys": [f"{key}_radio"]})
        builder.add_booleano_nullable(column, False)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_presence_radio(
    label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE Component for text/ID fields where value presence validates true flag."""
    cat_keys.append(f"{key}_radio")

    if f"{key}_radio" not in st.session_state:
        st.session_state[f"{key}_radio"] = "Both"

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )
    val = st.radio(
        label,
        ["Both", "Yes", "No"],
        horizontal=True,
        key=f"{key}_radio",
        label_visibility="collapsed",
    )

    if val == "Yes":
        ui_tracker.append({"text": f"{label}: Yes", "keys": [f"{key}_radio"]})
        builder.add_presenca(column, True)
    elif val == "No":
        ui_tracker.append({"text": f"{label}: No", "keys": [f"{key}_radio"]})
        builder.add_presenca(column, False)

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_dual_slider(
    use_case, label: str, column: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """SRE UX FIX: Bidirectional slider synchronized with numeric inputs for surgical precision."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    vmin, vmax = use_case.get_global_bounds(column, is_date=False)

    if pd.notna(vmin) and pd.notna(vmax) and vmin != vmax:
        vmin_val, vmax_val = int(vmin), int(vmax)

        if f"{key}_min" not in st.session_state:
            st.session_state[f"{key}_min"] = vmin_val
        if f"{key}_max" not in st.session_state:
            st.session_state[f"{key}_max"] = vmax_val
        if f"{key}_sld" not in st.session_state:
            st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

        st.write(
            f"<span class='sre-label'>{label}</span>",
            unsafe_allow_html=True,
        )

        def sync_slider():
            st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
            st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]

        def sync_num():
            safe_min = min(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            safe_max = max(
                st.session_state[f"{key}_min"], st.session_state[f"{key}_max"]
            )
            st.session_state[f"{key}_sld"] = (safe_min, safe_max)

        c1, c2 = st.columns(2)
        c1.number_input(
            "Minimum",
            min_value=vmin_val,
            max_value=vmax_val,
            key=f"{key}_min",
            on_change=sync_num,
            label_visibility="collapsed",
        )
        c2.number_input(
            "Maximum",
            min_value=vmin_val,
            max_value=vmax_val,
            key=f"{key}_max",
            on_change=sync_num,
            label_visibility="collapsed",
        )

        val = st.slider(
            label,
            vmin_val,
            vmax_val,
            key=f"{key}_sld",
            on_change=sync_slider,
            label_visibility="collapsed",
        )

        if val[0] > vmin_val or val[1] < vmax_val:
            ui_tracker.append(
                {
                    "text": f"{label}: {val[0]} a {val[1]}",
                    "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"],
                }
            )
            builder.add_limite_numerico(column, val[0], val[1])

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_age_slider(
    use_case, label: str, builder: FiltroAvancadoSpecBuilder, key: str, ui_tracker: list, cat_keys: list
):
    """Domain Component for Age: Converts visible Age Range to DATEDIFF in OLAP SQL."""
    cat_keys.extend([f"{key}_sld", f"{key}_min", f"{key}_max"])
    
    _policy = use_case._policy
    vmin_val, vmax_val = _policy.idade_min, _policy.idade_max

    if f"{key}_min" not in st.session_state:
        st.session_state[f"{key}_min"] = vmin_val
    if f"{key}_max" not in st.session_state:
        st.session_state[f"{key}_max"] = vmax_val
    if f"{key}_sld" not in st.session_state:
        st.session_state[f"{key}_sld"] = (vmin_val, vmax_val)

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )

    def sync_slider_age():
        st.session_state[f"{key}_min"] = st.session_state[f"{key}_sld"][0]
        st.session_state[f"{key}_max"] = st.session_state[f"{key}_sld"][1]

    def sync_num_age():
        safe_min = min(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        safe_max = max(st.session_state[f"{key}_min"], st.session_state[f"{key}_max"])
        st.session_state[f"{key}_sld"] = (safe_min, safe_max)

    c1, c2 = st.columns(2)
    c1.number_input(
        "Min Age",
        min_value=vmin_val,
        max_value=vmax_val,
        key=f"{key}_min",
        on_change=sync_num_age,
        label_visibility="collapsed",
    )
    c2.number_input(
        "Max Age",
        min_value=vmin_val,
        max_value=vmax_val,
        key=f"{key}_max",
        on_change=sync_num_age,
        label_visibility="collapsed",
    )

    val = st.slider(
        label,
        vmin_val,
        vmax_val,
        key=f"{key}_sld",
        on_change=sync_slider_age,
        label_visibility="collapsed",
    )

    if val[0] > vmin_val or val[1] < vmax_val:
        ui_tracker.append(
            {
                "text": f"{label}: {val[0]} to {val[1]} years",
                "keys": [f"{key}_sld", f"{key}_min", f"{key}_max"],
            }
        )
        builder.add_limite_numerico("entidade_idade_idadeInteiro", val[0], val[1])

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_smart_date_range(
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    default_to_30_days: bool = False,
):
    """SRE UX FIX: Exclusively uses the native Streamlit selector, which already brings Range and Presets built-in."""
    cat_keys.append(key)

    if key not in st.session_state:
        if default_to_30_days:
            hoje = date.today()
            st.session_state[key] = (hoje - timedelta(days=30), hoje)
        else:
            st.session_state[key] = ()

    st.write(
        f"<span class='sre-label'>{label}</span>",
        unsafe_allow_html=True,
    )

    val = st.date_input(label, key=key, label_visibility="collapsed")

    if isinstance(val, tuple) and len(val) == 2:
        ui_tracker.append(
            {
                "text": f"{label}: {val[0].strftime('%Y-%m-%d')} to {val[1].strftime('%Y-%m-%d')}",
                "keys": [key],
            }
        )
        builder.add_limite_data(column, val[0].strftime('%Y-%m-%d'), val[1].strftime('%Y-%m-%d'))

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_advanced_text_search(
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    aggregate_by: str = None,
    default_toggle: bool = False,
):
    """
    Renders a Toggle with Boolean logic, Accent tolerance, and Wildcard (*) support.
    If aggregate_by is passed, uses 'bool_or' (Single-pass OLAP).
    Added 'default_toggle' to allow Deep Search already open (Ex: Evolutions).
    """
    cat_keys.extend(
        [f"{key}_toggle", f"{key}_and_val", f"{key}_or_val", f"{key}_not_val"]
    )

    if f"{key}_toggle" not in st.session_state:
        st.session_state[f"{key}_toggle"] = default_toggle

    for suffix in ["and", "or", "not"]:
        if f"{key}_{suffix}_val" not in st.session_state:
            st.session_state[f"{key}_{suffix}_val"] = ""

    icon = "🧠" if aggregate_by else "🔎"
    is_active = st.toggle(f"{icon} Deep Search: {label}", key=f"{key}_toggle")

    if is_active:
        col_indent, col_content = st.columns([0.05, 0.95])

        with col_content:
            if aggregate_by:
                st.markdown(
                    "<div class='aggregate-search-bar'>Global Search: Looks into <b>all clinical history</b>.</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    "<div class='deep-search-bar'>Search within Event.</div>",
                    unsafe_allow_html=True,
                )

            st.caption(
                r"Separate by comma ( , ). Use **\*** as wildcard (ex: *cardio\**). Accents are ignored."
            )

            or_terms = st.text_input(
                "✅ Contains ANY (OR)",
                value=st.session_state[f"{key}_or_val"],
                key=f"{key}_or",
            )
            and_terms = st.text_input(
                "⚠️ Contains ALL (AND)",
                value=st.session_state[f"{key}_and_val"],
                key=f"{key}_and",
            )
            not_terms = st.text_input(
                "❌ DOES NOT contain (NOT)",
                value=st.session_state[f"{key}_not_val"],
                key=f"{key}_not",
            )

            if st.session_state.get(f"{key}_or_val", "") or or_terms:
                st.session_state[f"{key}_or_val"] = or_terms
            if st.session_state.get(f"{key}_and_val", "") or and_terms:
                st.session_state[f"{key}_and_val"] = and_terms
            if st.session_state.get(f"{key}_not_val", "") or not_terms:
                st.session_state[f"{key}_not_val"] = not_terms

            if and_terms or or_terms or not_terms:
                _or = [w for w in or_terms.split(",") if w.strip()] if or_terms else []
                _and = [w for w in and_terms.split(",") if w.strip()] if and_terms else []
                _not = [w for w in not_terms.split(",") if w.strip()] if not_terms else []

                if _or:
                    ui_tracker.append({"text": f"✅ {label}: {or_terms}", "keys": [f"{key}_or_val", f"{key}_or", f"{key}_toggle"]})
                if _and:
                    ui_tracker.append({"text": f"⚠️ AND {label}: {and_terms}", "keys": [f"{key}_and_val", f"{key}_and", f"{key}_toggle"]})
                if _not:
                    ui_tracker.append({"text": f"❌ {label}: {not_terms}", "keys": [f"{key}_not_val", f"{key}_not", f"{key}_toggle"]})

                builder.add_busca_avancada(
                    column=column,
                    or_terms=_or,
                    and_terms=_and,
                    not_terms=_not,
                    aggregate_by=aggregate_by if aggregate_by else None
                )

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_outcome_type_filter(
    use_case,
    label: str,
    column: str,
    builder: FiltroAvancadoSpecBuilder,
    current_where: str,
    key: str,
    ui_tracker: list,
    cat_keys: list,
    current_user,
):
    """Renders the unique Outcome Type filter handling IN PROGRESS meaning empty."""
    _desfecho_options_raw = use_case.get_dynamic_options(column, current_where, current_user)
    _desfecho_options = sorted(set([o for o in _desfecho_options_raw if o])) + ["IN PROGRESS"]

    cat_keys.extend([f"{key}_in", f"{key}_ex"])
    
    st.write(
        f"<span style='font-size:0.9em;font-weight:600;color:#4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )
    
    _sla_incl = st.multiselect(
        f"{label} ✅",
        _desfecho_options,
        key=f"{key}_in",
        label_visibility="collapsed",
        placeholder=f"✅ Include {label}...",
    )
    _sla_excl = st.multiselect(
        f"{label} ❌",
        _desfecho_options,
        key=f"{key}_ex",
        label_visibility="collapsed",
        placeholder=f"❌ Exclude {label}...",
    )
    
    if _sla_incl:
        ui_tracker.append(
            {"text": f"✅ {label}: {', '.join(_sla_incl)}", "keys": [f"{key}_in"]}
        )
        _parts = []
        if "IN PROGRESS" in _sla_incl:
            _rest = [v for v in _sla_incl if v != "IN PROGRESS"]
            _parts.append(f'("{column}" IS NULL OR "{column}" = \'\')')
            if _rest:
                _safe = "', '".join(v.replace("'", "''") for v in _rest)
                _parts.append(f'"{column}" IN (\'{_safe}\')')
        else:
            _safe = "', '".join(v.replace("'", "''") for v in _sla_incl)
            _parts.append(f'"{column}" IN (\'{_safe}\')')
        builder.add_clausula_legado(f"({' OR '.join(_parts)})")

    if _sla_excl:
        ui_tracker.append(
            {"text": f"❌ {label}: {', '.join(_sla_excl)}", "keys": [f"{key}_ex"]}
        )
        _parts_ex = []
        if "IN PROGRESS" in _sla_excl:
            _rest_ex = [v for v in _sla_excl if v != "IN PROGRESS"]
            _parts_ex.append(f'("{column}" IS NOT NULL AND "{column}" != \'\')')
            if _rest_ex:
                _safe_ex = "', '".join(v.replace("'", "''") for v in _rest_ex)
                _parts_ex.append(f'"{column}" NOT IN (\'{_safe_ex}\')')
        else:
            _safe_ex = "', '".join(v.replace("'", "''") for v in _sla_excl)
            _parts_ex.append(f'"{column}" NOT IN (\'{_safe_ex}\')')
        builder.add_clausula_legado(f"({' AND '.join(_parts_ex)})")

    return DuckDBCriteriaTranslator.translate(builder.build())


def render_pending_reasons_filter(
    use_case,
    label: str,
    builder: FiltroAvancadoSpecBuilder,
    current_where: str,
    ui_tracker: list,
    cat_keys: list,
    current_user,
):
    """Renders the special Pending Reasons filter extracting fields from JSON."""
    st.write(
        f"<span style='font-size:0.9em;font-weight:600;color:#4B5563;'>{label}</span>",
        unsafe_allow_html=True,
    )
    _pend_fields = [
        ("Type", "json_extract_string(\"motivoPendencia\", '$.tipo')", "mot_pend_tipo"),
        ("Reason", "json_extract_string(\"motivoPendencia\", '$.motivo')", "mot_pend_mot"),
        ("Description", "json_extract_string(\"motivoPendencia\", '$.descricao')", "mot_pend_desc"),
        ("Status", "json_extract_string(\"motivoPendencia\", '$.status')", "mot_pend_sta"),
    ]
    _where_for_pend = current_where if current_where.strip() else "1=1"
    
    for _pf_label, _pf_expr, _pf_key in _pend_fields:
        try:
            _pf_sql = (
                f"SELECT DISTINCT {_pf_expr} AS val "
                f"FROM gercon "
                f"WHERE {_where_for_pend} "
                f"AND {_pf_expr} IS NOT NULL "
                f"AND {_pf_expr} != '' "
                f"ORDER BY 1"
            )
            _pf_raw = use_case.execute_custom_query(_pf_sql, None, current_user)
            _pf_opts = _pf_raw["val"].dropna().tolist() if not _pf_raw.empty else []
        except Exception:
            _pf_opts = []

        if not _pf_opts:
            continue

        cat_keys.extend([f"{_pf_key}_in", f"{_pf_key}_ex"])
        st.caption(_pf_label)
        _pf_incl = st.multiselect(
            f"{_pf_label} ✅",
            sorted(set(str(o) for o in _pf_opts)),
            key=f"{_pf_key}_in",
            label_visibility="collapsed",
            placeholder=f"✅ Include {_pf_label}...",
        )
        _pf_excl = st.multiselect(
            f"{_pf_label} ❌",
            sorted(set(str(o) for o in _pf_opts)),
            key=f"{_pf_key}_ex",
            label_visibility="collapsed",
            placeholder=f"❌ Exclude {_pf_label}...",
        )
        
        if _pf_incl:
            _pf_safe = "', '".join(v.replace("'", "''") for v in _pf_incl)
            builder.add_clausula_legado(f"{_pf_expr} IN ('{_pf_safe}')")
            ui_tracker.append(
                {"text": f"✅ Pending {_pf_label}: {', '.join(_pf_incl)}", "keys": [f"{_pf_key}_in"]}
            )
        
        if _pf_excl:
            _pf_safe_ex = "', '".join(v.replace("'", "''") for v in _pf_excl)
            builder.add_clausula_legado(f"{_pf_expr} NOT IN ('{_pf_safe_ex}')")
            ui_tracker.append(
                {"text": f"❌ Pending {_pf_label}: {', '.join(_pf_excl)}", "keys": [f"{_pf_key}_ex"]}
            )

    return DuckDBCriteriaTranslator.translate(builder.build())
