#!/bin/bash
set -e
# Inicia a aplicação principal de forma limpa (direto no root)
exec streamlit run app_analytics.py --server.port=8501 --server.address=0.0.0.0
