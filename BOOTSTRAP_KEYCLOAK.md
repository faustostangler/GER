# 🔐 Guia de Bootstrap: Identidade Keycloak (SOTA)

Este guia detalha a configuração manual necessária no console do Keycloak para habilitar a malha de segurança Zero-Trust do projeto GER.

---

## 1. Criar o Realm (O Domínio do App)
O realm `master` é apenas para administração global. Vamos criar um espaço isolado para o projeto.

1. Acesse: **http://localhost:8080**
2. Login: Use as credenciais `KEYCLOAK_ADMIN` e `KEYCLOAK_ADMIN_PASSWORD` do seu `env/creds.env`.
3. No canto superior esquerdo, clique em **Master** ➔ **Create Realm**.
4. **Realm name**: `gercon-realm`.
5. Clique em **Create**.

---

## 2. Criar o Client (O "RG" do Proxy)
O `oauth2-proxy` precisa de um cadastro para se identificar perfeitamente.

1. No menu lateral, clique em **Clients** ➔ **Create client**.
2. **Client ID**: `gercon-analytics`.
3. Clique em **Next**.
4. **Capability config**:
   - **Client authentication**: Mude para **ON** (Isso ativa o Segredo/Secret).
   - Mantenha **Standard flow** e **Direct access grants** ativados.
5. Clique em **Next**.
6. **Login settings**:
   - **Valid redirect URIs**: `http://localhost/*` (Para desenvolvimento local).
   - **Web Origins**: `*`.
7. Clique em **Save**.

---

## 3. Capturar o Client Secret
Esta é a "senha" que o seu contêiner de Proxy usará para falar com o Keycloak.

1. Ainda no client `gercon-analytics`, clique na aba superior **Credentials**.
2. Copie o valor de **Client Secret**.
3. **Ação no Código**: Abra o seu arquivo `env/creds.env` e substitua o valor 
em OAUTH2_PROXY_CLIENT_SECRET=`colocar_secrect_quando_criar_o_client` por este código copiado.

---

## 4. Configurar os Atributos de CRM (Zero Trust Data)
Para que o gráfico do Streamlit filtre os dados por médico, o Token JWT precisa carregar o CRM.

### Passo A: Definir o Perfil do Usuário
1. No menu lateral, vá em **Realm settings** ➔ Aba **User profile**.
2. Clique em **Attributes** ➔ **Create attribute**.
3. Crie dois atributos exatamente com estes nomes:
   - `crm_numero`
   - `crm_uf`
4. Para ambos, marque as permissões de **View** e **Edit** para o usuário e clique em **Save**.

### Passo B: Mapear para o Token
Para que esses dados apareçam no "envelope" enviado ao Python:

1. Vá em **Clients** ➔ **gercon-analytics** ➔ Aba **Client scopes**.
2. Clique no link **gercon-analytics-dedicated**.
3. Clique em **Add mapper** ➔ **Configure a new mapper** ou **By configuration** ➔ **User Attribute**.
4. **Name**: `crm_numero_mapper` ou `crm_uf_mapper`.
5. **User Attribute**: `crm_numero`.
6. **Token Claim Name**: `crm_numero`.
7. Clique em **Save**. (Repita o processo para o `crm_uf`).

## 5. Provisionar a Primeira Identidade (Primeiro Usuário)
A criação do seu primeiro usuário "real" é a última etapa do seu Bootstrap para validar o fluxo ponta-a-ponta.

1. No canto superior esquerdo, certifique-se de que o **`gercon-realm`** está selecionado (nunca use o `master` para usuários da aplicação).
2. No menu lateral, clique em **Users** ➔ **Create new user**.
3. Preencha os campos essenciais:
   - **Username**: (ex: `medico_teste`)
   - **Email**: (ex: `teste@hospital.com`)
   - **First Name** e **Last Name**.
   - **CRM Number** e **CRM State**.
4. Clique em **Create**.
5. Vá até a aba superior **Credentials** ➔ clique em **Set password**.
6. **SRE DX Hack**: Digite a senha e **DESLIGUE** a chave **Temporary**. Isso evita que o Keycloak force a troca de senha no primeiro login, acelerando imensamente o ciclo de testes locais. Ou deixe ligado para segurança do usuário. 
7. Clique em **Save** e confirme.

---

## 🚀 Próximos Passos
Após atualizar o `env/creds.env` com o seu **Client Secret**, você está pronto para subir a malha completa:

```bash
make up-iam
```
