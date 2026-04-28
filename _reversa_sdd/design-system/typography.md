# Tipografia

Sistema tipográfico extraído do arquivo `src/presentation/static/custom_style.css`.

## Famílias de Fontes

| Token | Família | Uso | Confiança |
| :--- | :--- | :--- | :--- |
| `--font-display` | `'Inter', sans-serif` | Títulos e grandes números (KPIs) | 🟢 |
| `--font-body` | `'Inter', sans-serif` | Textos gerais, labels e formulários | 🟢 |

> [!NOTE]
> A fonte **Inter** é carregada externamente via Google Fonts (pesos: 300, 400, 500, 600, 700, 800, 900).

## Hierarquia e Estilos

| Seletor / Componente | Tamanho | Peso | Letter-spacing | Cor | Confiança |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `.sre-title` | `3.8rem` | 900 | `-0.04em` | `#ffffff` | 🟢 |
| `.sre-subtitle` | `0.9rem` | 500 | `0.4em` | `var(--primary)` | 🟢 |
| `.kpi-value` | `3.2rem` | 800 | `-0.02em` | `#ffffff` | 🟢 |
| `.kpi-label` | `0.7rem` | 700 | `0.15em` | `var(--on-surface-variant)` | 🟢 |
| `.amber-alert-title` | `1.2rem` | 800 | `0.05em` | `var(--tertiary)` | 🟢 |
| `.sre-label` | `0.75rem` | 700 | `0.1em` | `var(--on-surface-variant)` | 🟢 |
| `.sre-section-title` | `0.9rem` | 800 | `0.2em` | `var(--primary)` | 🟢 |

## Regras Gerais
- Todo o texto da aplicação força o uso da fonte `Inter` via `!important` no seletor geral `html, body, [class*="css"]`.
- Textos de cabeçalho e labels frequentemente utilizam `text-transform: uppercase` para reforçar a estética clínica/técnica.
