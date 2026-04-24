import streamlit as st
from presentation.components.filters import clear_filter_state


def render_active_filters_top_bar(ui_filters: dict, state_keys: dict):
    """
    Renders the top bar displaying currently active semantic filters.
    Allows the user to clear individual filters or all of them at once.

    Args:
        ui_filters (dict): A dictionary mapping categories to lists of active filter dictionaries.
        state_keys (dict): A dictionary mapping categories to Streamlit session state keys.
    """
    has_active_filters = any(len(v) > 0 for v in ui_filters.values())

    if has_active_filters:
        total_count = sum(len(v) for v in ui_filters.values())

        with st.expander(f"🔍 Active Filters ({total_count})", expanded=True):
            for category, filters in ui_filters.items():
                if filters:
                    # 1. TITLE ON ITS OWN LINE
                    st.markdown(
                        f"<div class='cat-title'>{category}</div>",
                        unsafe_allow_html=True,
                    )

                    # 2. FILTERS GROUPED ON THE NEXT LINE
                    with st.container():
                        st.markdown(
                            "<div class='filter-row-marker' style='display:none;'></div>",
                            unsafe_allow_html=True,
                        )
                        for i, f in enumerate(filters):
                            st.button(
                                f"{f['text']}",
                                key=f"clr_item_{category}_{i}",
                                on_click=clear_filter_state,
                                args=(f["keys"],),
                            )

            # 3. CLEAR ALL ISOLATED AT THE END
            st.write("")  # Natural micro-spacing
            all_keys = [key for sublist in state_keys.values() for key in sublist]
            st.button(
                "🗑️ Clear All Filters",
                key="btn_clear_all",
                on_click=clear_filter_state,
                args=(all_keys,),
                type="primary",
            )

        st.write(" ")  # A micro-spacing right before KPIs to breathe
