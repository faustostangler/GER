# Espaçamento e Layout

Padrões de espaçamento, grid e layout extraídos de `src/presentation/static/custom_style.css`.

## Escala de Espaçamento (Inferida)

Embora o sistema não utilize variáveis CSS específicas para espaçamento, os seguintes padrões são observados nos componentes customizados:

| Uso | Valor | Contexto | Confiança |
| :--- | :--- | :--- | :--- |
| Padding Interno (Grandes) | `2.5rem` / `3rem` | Cabeçalho (`.sre-header-container`), Violação de Auth | 🟢 |
| Padding Interno (Padrão) | `2rem` | KPI Cards, Amber Alert | 🟢 |
| Padding Interno (Pequeno)| `1rem` | Search Bars, Inputs, Auth Meta | 🟢 |
| Padding Interno (Micro)  | `0.5rem` | Inputs (text/select) | 🟢 |
| Margem Externa | `2rem` / `2.5rem`| Margem entre seções grandes | 🟢 |
| Gap entre elementos | `1.5rem` | Alinhamento flex (ícones + texto) | 🟢 |

## Grid e Breakpoints

A aplicação utiliza o layout responsivo nativo do **Streamlit**, complementado por ajustes no CSS.

- **Layout Base**: Flexbox e Grid automáticos do Streamlit.
- **Max-width**: O container de violação de autenticação (`.auth-violation-container`) possui um limite fixo de `600px` centralizado.

## Z-Index

Não foram identificadas manipulações explícitas de `z-index` no arquivo de estilos customizados. O empilhamento segue o fluxo padrão do DOM gerado pelo Streamlit.
