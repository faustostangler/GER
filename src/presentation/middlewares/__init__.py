# WHY: Package marker for the presentation.middlewares layer.
# Middlewares are presentation-level ACLs that adapt identity/session concerns
# before they reach the core rendering logic. They act as the seam between
# the IAM infrastructure adapter (streamlit_auth.py) and the main app entry point.
# Ref: ADR-006 — IAM Adapter Isolation (Phase 3 / SRP extraction).
