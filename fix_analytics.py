import re

with open('app_analytics.py', 'r') as f:
    lines = f.readlines()

new_lines = []
skip = False
for i, line in enumerate(lines):
    if line.startswith("# --- 3. STATE MANAGEMENT ---"):
        skip = True
        new_lines.append("from src.presentation.components.filters import clear_filter_state, render_include_exclude, render_boolean_radio, render_presence_radio, render_dual_slider, render_age_slider, render_smart_date_range, render_advanced_text_search\n")
        new_lines.append("from src.presentation.components.alerts import render_amber_alert\n")

    if not skip:
        # replace usages
        line = re.sub(r'render_include_exclude\(', r'render_include_exclude(use_case, ', line)
        line = re.sub(r'render_dual_slider\(', r'render_dual_slider(use_case, ', line)
        line = re.sub(r'render_age_slider\(', r'render_age_slider(use_case, ', line)
        new_lines.append(line)
        
    if line.startswith("    # --- State 3: Data is fresh — no banner rendered (happy path) ---"):
        skip = False

with open('app_analytics.py', 'w') as f:
    f.writelines(new_lines)
