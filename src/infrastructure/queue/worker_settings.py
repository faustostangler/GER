# src/infrastructure/queue/worker_settings.py
from arq import cron
from arq.connections import RedisSettings
import asyncio

# SRE FIX: Importe o Job pesado (Scraper Main Pipeline)
# from src.application.scripts.worker import principal_sync_pipeline -> Ou adapte a injeção do seu worker.py

async def run_daily_sync(ctx):
    print("🚀 Iniciando processamento resiliente via Arq...")
    
    # ATENÇÃO: Envelopamento try-except explícito com DLQ handling
    try:
        # Importe localmente para evitar overhead de carga na RAM do Worker se inativo
        # Para efeito do plano MVP do Arq chamamos o processo como Subprocesso ou Function Call
        import subprocess
        # Chamada assíncrona para não colidir o EventLoop nativo
        process = await asyncio.create_subprocess_exec(
            "python", "worker.py",
            env={"HEADLESS": "True"}
        )
        await process.communicate()
        
        if process.returncode != 0:
            raise Exception(f"Worker falhou nativamente, com exit_code {process.returncode}")
            
    except Exception as e:
        print(f"❌ Falha massiva no processamento do Arq Sync: {e}")
        raise # Joga para Arq gerenciar as 3 tentativas e Timeout Error
        
    print("✅ Processamento finalizado com sucesso!")

class WorkerConfig:
    # Agendamento Ativo SOTA nativo sem dependência do Scheduler do Docker
    cron_jobs = [
        # Roda todo dia as 03:00 da manhã
        cron(run_daily_sync, hour=3, minute=0)
    ]
    
    redis_settings = RedisSettings(host='ger-redis-queue', port=6379)
    functions = [run_daily_sync]
    
    # Propriedades de resiliência e fail-safe DLQ:
    max_tries = 3 # Tolerância a falhas na B3 ou gargalo de I/O em banco
    job_timeout = 3600 # 1 Hora máxima para a raspagem
