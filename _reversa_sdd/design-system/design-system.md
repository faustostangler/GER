# Design System: Search Essence x Digital Clinical Surgeon

Documento consolidado que descreve a identidade visual e os princípios de design da aplicação GER.

## Filosofia do Design
A estética visual do GER é definida como **High-Velocity Intelligence, Surgical Precision, Nocturnal Depth**. Ela busca transmitir:
1. **Profundidade Noturna**: Uso de temas escuros profundos para reduzir a fadiga visual de clínicos e analistas.
2. **Precisão Cirúrgica**: Destaques visuais claros e dados organizados em cartões com Glassmorphism.
3. **Velocidade**: Efeitos de transição suaves e micro-interações (hover).

## Arquitetura Visual

O sistema visual é construído sobre o framework **Streamlit**, fortemente customizado através do arquivo de estilos [custom_style.css](file:///home/stangler/Documents/Python/GER/src/presentation/static/custom_style.css).

### Componentes Principais

1. **Clinical Header**: Títulos grandes com forte contraste e uma barra de destaque colorida abaixo.
2. **Surgical KPI Cards**: Containers com efeito de vidro fosco (`backdrop-filter: blur`), sombras profundas e elevação ao passar o mouse.
3. **Amber Alert**: Seção de alerta clínico com transparência e cores que chamam a atenção sem poluir a tela.
4. **Zero-Trust Gate**: Interface de bloqueio de acesso com forte identidade visual vermelha para indicar violações de segurança.

## Estrutura de Arquivos

A documentação detalhada dos tokens visuais está dividida em:
- [Paleta de Cores](file:///home/stangler/Documents/Python/GER/_reversa_sdd/design-system/color-palette.md)
- [Tipografia](file:///home/stangler/Documents/Python/GER/_reversa_sdd/design-system/typography.md)
- [Espaçamento e Layout](file:///home/stangler/Documents/Python/GER/_reversa_sdd/design-system/spacing.md)
- [Dicionário de Tokens](file:///home/stangler/Documents/Python/GER/_reversa_sdd/design-system/tokens.md)

## Próximos Passos Recomendados
- Migrar os tokens CSS inline do Streamlit para componentes reutilizáveis.
- Padronizar a biblioteca de componentes para garantir consistência visual em novas telas.
