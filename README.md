# 🏗️ Data Pipeline – Arquitetura Medallion com Dagster + PostgreSQL

## 📌 Visão Geral

Este projeto implementa um pipeline completo de Engenharia de Dados utilizando:

- Python
- Dagster (Asset-Based Orchestration)
- PostgreSQL
- Docker
- Arquitetura Medallion (Bronze → Silver → Gold)

A solução foi construída com foco em:

- Separação clara de responsabilidades
- Governança de dados
- Escalabilidade
- Reprocessamento seguro
- Preparação para consumo analítico (BI)

---

# 🧱 Arquitetura Medallion

O pipeline segue o padrão arquitetural em camadas progressivas:

API SERPRO  
↓  
Bronze (Ingestão e Persistência Bruta)  
↓  
Silver (Tratamento e Regras de Negócio)  
↓  
Gold (Agregação e Estrutura Analítica)

Cada camada possui responsabilidades bem definidas e independentes.

---

# 📁 Estrutura de Diretórios

workspace/
│
├── assets/
│   ├── bronze/
│   │   └── bronze_promocoes.py
│   │
│   ├── silver/
│   │   ├── silver_nmpr.py
│   │   └── silver_mod.py
│   │
│   └── gold/
│       ├── dim_gold_nmpr_sit.py
│       └── gold_mod.py
│
├── resources/
│   └── api_client.py
│
├── definitions.py
├── docker-compose.yml
└── Dockerfile

Organização por camada permite:

✔ Escalabilidade  
✔ Governança  
✔ Evolução modular  
✔ Manutenção facilitada  

---

# 🥉 Bronze Layer — Ingestão e Persistência

## Asset: bronze_promocoes

### Objetivo

Capturar dados diretamente da API oficial e persistir no PostgreSQL mantendo fidelidade à origem.

### Processo Técnico

1. Consumo da API REST.
2. Conversão do JSON para estrutura tabular.
3. Criação da tabela se não existir.
4. Inserção em lote utilizando execute_values.
5. Registro de materialização no Dagster.

### Estrutura Persistida

Tabela criada:

promocoes_bronzer

Colunas:

- numero_promocao
- nome_promocao
- modalidade
- situacao
- data_inicio
- data_fim

### Características da Bronze

- Dados próximos ao formato original.
- Sem aplicação de regras de negócio complexas.
- Persistência transacional.
- Base para auditoria.
- Permite reprocessamento completo.

---

# 🥈 Silver Layer — Tratamento e Regras de Negócio

A Silver transforma dados operacionais em dados estruturados e padronizados.

---

## Asset: silver_nmpr

### Objetivo

Selecionar apenas promoções com status AUTORIZADA.

### Regras Aplicadas

- Filtro por situação = 'AUTORIZADA'
- Remoção de duplicidade (DISTINCT)
- Padronização textual (UPPER)
- Inclusão de timestamp técnico (NOW())

### Resultado

Tabela:

nmpr_sit_autz

Contém apenas dados validados para análise.

---

## Asset: silver_mod

### Objetivo

Derivar atributos temporais da data final da promoção.

### Regras Aplicadas

- Padronização de modalidade (UPPER)
- Extração do mês da data_fim
- Extração do dia da data_fim

### Finalidade

Preparação para futura dimensão temporal e modelagem analítica.

---

# 🥇 Gold Layer — Consolidação e Modelo Analítico

A Gold consolida dados já tratados e aplica agregações.

Aqui os dados passam a ter foco analítico e gerencial.

---

## Asset: dim_gold_nmpr_sit

### Objetivo

Gerar agregação por nome de promoção.

### Regra Aplicada

- Agrupamento por nome_promocao
- Contagem de registros

### Finalidade Analítica

- Medir volume por promoção
- Apoiar relatórios gerenciais
- Identificar concentração de campanhas

---

## Asset: gold_mod

### Objetivo

Gerar agregação por modalidade considerando apenas promoções autorizadas.

### Regra Aplicada

- Agrupamento por nm_mod
- Contagem de registros
- Base apenas na tabela validada da Silver

### Resultado

Tabela:

dim_gold_mod

Colunas:

- nm_mod
- qtd_mod

---

# 🔍 Governança e Engenharia

## Orquestração via Dagster Assets

Cada camada é implementada como @asset, permitindo:

- Observabilidade
- Controle de dependência
- Materialização explícita
- Reprocessamento seletivo

## Controle Transacional

- Conexão explícita com PostgreSQL
- Commit manual
- Encerramento seguro de cursor
- Encerramento seguro de conexão

## Idempotência

Uso de CREATE TABLE IF NOT EXISTS permite reexecução segura do pipeline.

---

# 🚀 Benefícios Arquiteturais

- Pipeline modular
- Separação clara de responsabilidade
- Preparado para cloud
- Estrutura evolutiva
- Compatível com ferramentas de BI
- Base para modelagem dimensional futura

---

# 🔮 Evoluções Futuras

- Implementação de dimensão tempo
- Implementação de surrogate keys
- SCD Type 2
- Separação formal entre fato e dimensão
- Particionamento de tabelas
- Carga incremental

---

# 👨‍💻 Autor

Walney de Negreiros Gomes  
Data Engineer | BI Specialist | Cloud & Streaming

Especialidades:

- Apache Spark
- Azure & AWS
- Engenharia de Dados
- Modelagem Analítica
- Orquestração de Pipelines
