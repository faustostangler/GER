#!/bin/bash
set -e
# Inicia o Nginx em background
nginx -g "daemon off;" &
# Inicia a aplicação principal
exec streamlit run app_analytics.py --server.port=8501 --server.address=0.0.0.0 --server.baseUrlPath=dashboard
