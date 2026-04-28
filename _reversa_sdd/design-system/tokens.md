# Dicionário de Tokens de Design

Consolidação de todos os tokens visuais identificados no projeto GER.

## Tokens Globais (Design Tokens)

| Categoria | Nome do Token | Valor | Tipo | Confiança |
| :--- | :--- | :--- | :--- | :--- |
| Cor | `--background` | `#0d1322` | Hex | 🟢 |
| Cor | `--surface` | `rgba(25, 31, 47, 0.7)` | RGBA | 🟢 |
| Cor | `--surface-high` | `rgba(36, 42, 58, 0.85)` | RGBA | 🟢 |
| Cor | `--primary` | `#adc6ff` | Hex | 🟢 |
| Cor | `--primary-glow` | `rgba(173, 198, 255, 0.25)` | RGBA | 🟢 |
| Cor | `--secondary` | `#6ddd81` | Hex | 🟢 |
| Cor | `--tertiary` | `#fbbc06` | Hex | 🟢 |
| Cor | `--error` | `#ffb4ab` | Hex | 🟢 |
| Cor | `--on-surface` | `#dde2f8` | Hex | 🟢 |
| Cor | `--on-surface-variant`| `#8c909f` | Hex | 🟢 |
| Tipografia | `--font-display` | `'Inter', sans-serif` | String | 🟢 |
| Tipografia | `--font-body` | `'Inter', sans-serif` | String | 🟢 |
| Bordas | `--glass-border` | `rgba(255, 255, 255, 0.05)` | RGBA | 🟢 |
| Efeitos | `--glass-shadow` | `0 12px 48px 0 rgba(0, 0, 0, 0.4)` | String | 🟢 |

## Tokens Semânticos e Componentes

| Componente | Propriedade | Valor | Confiança |
| :--- | :--- | :--- | :--- |
| `.kpi-card` | `border-radius` | `1rem` | 🟢 |
| `.kpi-card` | `backdrop-filter` | `blur(24px)` | 🟢 |
| `.kpi-card:hover` | `transform` | `translateY(-8px)` | 🟢 |
| `.amber-alert-container`| `border-radius` | `1.25rem` | 🟢 |
| `.amber-alert-container`| `backdrop-filter` | `blur(16px)` | 🟢 |
| `.stTextInput input` | `border-radius` | `12px` | 🟢 |
| `[data-testid="stHeader"]`| `backdrop-filter`| `blur(12px)` | 🟢 |

## Escala de Confiança
- 🟢 **Extraído de arquivo de configuração**: Valores definidos diretamente no código CSS.
- 🟡 **Inferido de uso**: Padrões observados na interface.
- 🔴 **Token referenciado mas não definido**: Não encontrado.
