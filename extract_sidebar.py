import os

with open("app_analytics.py", "r") as f:
    lines = f.readlines()

start_idx = -1
end_idx = -1

for i, line in enumerate(lines):
    if line.strip().startswith("ui_filters = {"):
        if start_idx == -1:
            start_idx = i
    if line.strip() == "# VISUALIZE AND CLEAR ACTIVE FILTERS (TOP BAR)":
        # Walk back 2 lines to include the space and the "================="
        end_idx = i - 2
        break

extracted_lines = lines[start_idx:end_idx]

os.makedirs("src/presentation/builders", exist_ok=True)
with open("src/presentation/builders/sidebar_builder.py", "w") as f:
    f.write("import streamlit as st\n")
    f.write(
        "from infrastructure.repositories.criteria_translator import DuckDBCriteriaTranslator\n"
    )
    f.write(
        "from presentation.components.filters import render_include_exclude, render_boolean_radio, render_presence_radio, render_dual_slider, render_age_slider, render_smart_date_range, render_advanced_text_search, render_outcome_type_filter, render_pending_reasons_filter\n\n"
    )
    f.write("def build_sidebar(use_case, builder, st_user):\n")
    f.write(
        '    """\n    Constrói a barra lateral de filtros em cascata e retorna os estados visuais.\n    """\n'
    )

    for line in extracted_lines:
        new_line = line.replace("st.session_state.user", "st_user")
        f.write("    " + new_line if new_line.strip() else "\n")

    f.write("\n    return ui_filters, state_keys, curr_where\n")

with open("app_analytics.py", "w") as f:
    for i, line in enumerate(lines):
        if (
            "from presentation.components.filters import clear_filter_state, render_include_exclude, render_boolean_radio, render_presence_radio, render_dual_slider, render_age_slider, render_smart_date_range, render_advanced_text_search, render_outcome_type_filter, render_pending_reasons_filter"
            in line
        ):
            f.write("from presentation.components.filters import clear_filter_state\n")
            f.write("from presentation.builders.sidebar_builder import build_sidebar\n")
        elif i == start_idx:
            f.write(
                "    # 🪄 A mágica arquitetural: Centenas de linhas viraram uma só.\n"
            )
            f.write(
                "    ui_filters, state_keys, curr_where = build_sidebar(use_case, builder, st.session_state.user)\n\n"
            )
        elif start_idx < i < end_idx:
            pass
        else:
            f.write(line)

print("Surgical extraction complete.")
