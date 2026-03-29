# 🔐 Guia de Bootstrap: Identidade Keycloak (SOTA)

Este guia detalha a configuração manual necessária no console do Keycloak para habilitar a malha de segurança Zero-Trust do projeto GER.

---

## 🔐 O Bounded Context de Identity (IaC / GitOps)

Nossa arquitetura utiliza a filosofia de **Infraestrutura como Código (IaC)** baseada nos princípios *Keep It Simple, Stupid (KISS)* e *12-Factor App*. 

**Você NÃO precisa configurar o Realm ou o Client manualmente.**

O estado de autorização da aplicação, o client `gercon-analytics` e as *Redirect URIs* do Split-Horizon DNS já estão declarados no arquivo estático localizado em `infra/identity/gercon-realm-export.json`.

### Como Funciona:
Ao executar o comando `make up-iam`, o Docker injeta esse JSON diretamente no diretório `/opt/keycloak/data/import/` do motor Quarkus. A flag `--import-realm` garante que o Keycloak aplique o estado exato deste arquivo na inicialização.

### Day-2 Operations (Evoluindo a configuração):
Se você precisar adicionar novas *roles*, novos *clients* ou mapeadores (OIDC Audience Mappers) durante o desenvolvimento:
1. Faça a alteração via GUI no Keycloak local.
2. Vá em **Realm settings -> Partial export**.
3. Sobrescreva o arquivo `infra/identity/gercon-realm-export.json` com a nova exportação.
4. Faça o *commit* no repositório.

*Nota para Produção:* Em ambientes definitivos, a injeção inicial do JSON resolve o Bootstrap. A evolução do estado será assumida pelos módulos do Terraform (`terraform/main.tf`) atuando como o Keycloak Provider no cluster EKS via ArgoCD.

---

## 🚀 Próximos Passos
Após atualizar o `env/creds.env` com o seu **Client Secret**, você está pronto para subir a malha completa:

```bash
make up-iam
```

### 🔗 Ponto de Entrada do Sistema
Após subir a malha, o seu Bounded Context de Analytics estará disponível em:
👉 **http://127.0.0.1.nip.io**
