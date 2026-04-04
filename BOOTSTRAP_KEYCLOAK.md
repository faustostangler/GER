# 🔐 Guia de Bootstrap: Identidade Keycloak (SOTA)

Este guia detalha as duas formas de configurar o Keycloak para habilitar a malha de segurança Zero-Trust do projeto GER.

---

## 🛠️ Opção 1: Configuração Automática (Recomendada / IaC)

Nossa arquitetura utiliza a filosofia de **Infraestrutura como Código (IaC)**. O estado do Realm, Clients e Redirect URIs já está declarado em `infra/identity/gercon-realm-export.json`.

1. **Inicie o Sistema Completo**:
   ```bash
   make up-iam
   ```
2. **O que acontece**: O Docker injeta o JSON em `/opt/keycloak/data/import/` e a flag `--import-realm` aplica as configurações automaticamente no boot.
3. **Ponto de Entrada**: Acesse [http://127.0.0.1.nip.io](http://127.0.0.1.nip.io) para testar o Analytics já protegido.

---

## 🖱️ Opção 2: Configuração Manual (Passo a Passo)

Use esta opção se desejar configurar o ambiente do zero ou entender os componentes internos.

### 0. Inicie apenas o Identity Provider
```bash
make bootstrap
```
Acesse: [http://127.0.0.1.nip.io:8080](http://127.0.0.1.nip.io:8080) e faça login com as credenciais `admin_stangler` / `pass_stangler` (definidas no seu `env/creds.env`).

### 1. Criar o Realm (O Domínio do App)
O realm `master` é apenas para administração global. Vamos criar um espaço isolado.
1. No canto superior esquerdo, clique em **Master** ➔ **Create Realm**.
2. **Realm name**: `gercon-realm`.
3. Clique em **Create**.

### 2. Criar o Client (O "RG" do Proxy)
O `oauth2-proxy` precisa de um cadastro para se identificar.
1. No menu lateral, clique em **Clients** ➔ **Create client**.
2. **Client ID**: `gercon-analytics`.
3. Clique em **Next**.
4. **Capability config**:
   - **Client authentication**: Mude para **ON** (Ativa o Segredo/Secret).
   - Mantenha **Standard flow** e **Direct access grants** ativados.
5. Clique em **Next**.
6. **Login settings**:
   - **Valid redirect URIs**: `http://localhost/*` e `http://127.0.0.1.nip.io/*`.
   - **Web Origins**: `*`.
7. Clique em **Save**.

### 3. Capturar o Client Secret
1. No client `gercon-analytics`, clique na aba **Credentials**.
2. Copie o valor de **Client Secret**.
3. **Ação no Código**: Abra o seu arquivo `env/creds.env` e atualize `OAUTH2_PROXY_CLIENT_SECRET` e `KEYCLOAK_CLIENT_SECRET` com este código.

### 4. Configurar os Atributos de CRM (Zero Trust Data)
Para que o Analytics filtre os dados por médico, o Token JWT precisa carregar o CRM.

**Passo A: Definir o Perfil do Usuário**
1. Vá em **Realm settings** ➔ Aba **User profile** ➔ **Attributes** ➔ **Create attribute**.
2. Crie `crm_numero` e `crm_uf`. Marque **View** e **Edit** para o usuário e clique em **Save**.

**Passo B: Mapear para o Token**
1. Vá em **Clients** ➔ **gercon-analytics** ➔ Aba **Client scopes** ➔ Link **gercon-analytics-dedicated**.
2. Clique em **Add mapper** ➔ **By configuration** ➔ **User Attribute**.
3. Configure `crm_numero` e `crm_uf` (Name, User Attribute e Claim Name iguais). Salve.

**Passo C: OIDC Audience Mapper**
1. Na mesma aba **Client scopes** ➔ **gercon-analytics-dedicated** ➔ **Add mapper** ➔ **By configuration** ➔ **Audience**.
2. **Included Client Audience**: Selecione `gercon-analytics`. Ligue `Add to access token`. Salve.

### 5. Provisionar a Primeira Identidade
1. Certifique-se de estar no **gercon-realm**.
2. Clique em **Users** ➔ **Create new user**.
3. Preencha os campos (Username, Email, etc). Clique em **Create**.
4. Aba **Credentials** ➔ **Set password**. **DESLIGUE** a chave **Temporary** para agilizar seus testes. Clique em **Save**.

---

## 🚀 Finalizando o Setup
Após qualquer configuração manual, você pode consolidar o estado executando:
```bash
make up-iam
```
👉 Acesse a aplicação em: **http://127.0.0.1.nip.io**
