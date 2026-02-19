# 🏗️ Data Engineering Pipeline  
## Arquitetura Medallion com Dagster + PostgreSQL + Docker

---

# 📌 Visão Estratégica do Projeto

Este projeto implementa um pipeline de Engenharia de Dados baseado no padrão **Medallion Architecture**, utilizando orquestração moderna via **Dagster Assets** e persistência transacional em **PostgreSQL**.

A solução foi construída com foco em:

- Separação clara de responsabilidades por camada
- Governança e rastreabilidade
- Idempotência estrutural
- Escalabilidade arquitetural
- Preparação para modelagem dimensional
- Organização para ambientes cloud-ready

---

# 🧱 Arquitetura Medallion

A arquitetura é estruturada em três camadas progressivas de refinamento:

```
Fonte de Dados (API SERPRO)
          ↓
Bronze  → Persistência Bruta
          ↓
Silver  → Tratamento e Regras de Negócio
          ↓
Gold    → Consolidação e Agregação Analítica
```

Cada camada possui responsabilidades técnicas específicas e bem delimitadas.

---

# 📁 Estrutura de Diretórios

A organização física do projeto segue o princípio de separação por domínio e responsabilidade:

```bash
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
```

## 🎯 Objetivo da Estrutura

- `bronze/` → Camada de ingestão bruta  
- `silver/` → Camada de padronização e aplicação de regras  
- `gold/` → Camada de consolidação e agregação analítica  
- `resources/` → Conexões externas e integrações  
- `definitions.py` → Registro central de assets no Dagster  

Essa organização permite:

✔ Modularidade  
✔ Facilidade de manutenção  
✔ Evolução incremental  
✔ Escalabilidade para múltiplas fontes  
✔ Governança clara  

---

# 🥉 Bronze Layer — Ingestão e Persistência

## 📌 Asset: `bronze_promocoes`

### 🎯 Objetivo

Realizar a ingestão de dados diretamente da API oficial do SERPRO e persistir no PostgreSQL preservando a estrutura original.

### 🔄 Processo Técnico

1. Consumo da API REST via `requests`.
2. Conversão do JSON em estrutura compatível com carga relacional.
3. Criação da tabela caso não exista.
4. Inserção em lote utilizando `execute_values`.
5. Commit transacional explícito.
6. Registro de materialização via Dagster.

### 🗄️ Estrutura da Tabela

```sql
CREATE TABLE IF NOT EXISTS promocoes_bronzer (
    numero_promocao TEXT,
    nome_promocao TEXT,
    modalidade TEXT,
    situacao TEXT,
    data_inicio DATE,
    data_fim DATE
)
```

### 🧠 Características da Bronze

- Dados ainda não transformados.
- Sem aplicação de regras de negócio complexas.
- Estrutura preparada para auditoria.
- Permite reprocessamento completo.
- Persistência física transacional.

A Bronze representa a camada de **segurança e rastreabilidade do pipeline**.

---

# 🥈 Silver Layer — Tratamento e Regras de Negócio

A Silver é responsável por transformar dados operacionais em dados estruturados e analíticos.

---

## 📌 Asset: `silver_nmpr`

### 🎯 Objetivo

Selecionar exclusivamente promoções com status **AUTORIZADA**.

### 🔄 Transformação Aplicada

```sql
CREATE TABLE IF NOT EXISTS nmpr_sit_autz AS
SELECT DISTINCT 
    UPPER(numero_promocao) as nm_pr, 
    situacao as nm_sit, 
    UPPER(modalidade) as nm_mod,
    NOW() as dt_incl
FROM promocoes_bronzer 
WHERE situacao = 'AUTORIZADA'
```

### 📌 Regras de Negócio Aplicadas

- Filtro por situação AUTORIZADA.
- Remoção de registros duplicados.
- Padronização textual (UPPER).
- Inclusão de coluna técnica de controle (`dt_incl`).

### 🎯 Impacto Analítico

Reduz ruído operacional e garante que apenas dados válidos avancem para modelagem.

---

## 📌 Asset: `silver_mod`

### 🎯 Objetivo

Derivar atributos temporais da data final da promoção.

### 🔄 Transformação Aplicada

```sql
CREATE TABLE IF NOT EXISTS mod AS
SELECT 
    UPPER(modalidade) as nm_mod,
    EXTRACT(MONTH FROM data_fim) AS ms_fm,
    EXTRACT(DAY FROM data_fim) AS dt_fm
FROM promocoes_bronzer
```

### 📌 Regras Aplicadas

- Normalização de modalidade.
- Extração de mês da data final.
- Extração de dia da data final.

### 🎯 Objetivo Estratégico

Preparação para:

- Dimensão de tempo.
- Análises sazonais.
- Estruturação futura de modelo estrela.

---

# 🥇 Gold Layer — Consolidação e Estrutura Analítica

A Gold é a camada final do pipeline e possui foco analítico.

Aqui os dados já passaram por:

✔ Ingestão  
✔ Padronização  
✔ Regras de negócio  
✔ Limpeza  

Agora passam por:

✔ Agregação  
✔ Consolidação  
✔ Estrutura otimizada para BI  

---

## 📌 Asset: `dim_gold_nmpr_sit`

### 🎯 Objetivo

Consolidar volume de registros por nome de promoção.

### 🔄 Transformação

```sql
CREATE TABLE IF NOT EXISTS dim_gold_nmpr_sit AS
SELECT nome_promocao, COUNT(*) 
FROM promocoes_bronzer 
GROUP BY nome_promocao
```

### 📊 Resultado

Tabela agregada contendo:

- nome_promocao
- quantidade total de registros

### 📈 Aplicação Analítica

- Métricas de volume por campanha
- Indicadores estratégicos
- Base para dashboards executivos

---

## 📌 Asset: `gold_mod`

### 🎯 Objetivo

Consolidar volume por modalidade considerando apenas promoções autorizadas.

### 🔄 Transformação

```sql
CREATE TABLE IF NOT EXISTS dim_gold_mod AS
SELECT nm_mod, COUNT(*) as qtd_mod 
FROM nmpr_sit_autz 
GROUP BY nm_mod
```

### 📊 Resultado

Tabela:

- nm_mod
- qtd_mod

### 📌 Regra de Negócio Implícita

A Gold utiliza exclusivamente dados previamente validados na Silver, garantindo consistência e integridade analítica.

---

# 🔍 Governança e Engenharia

## ✔ Orquestração via Dagster

Cada camada é implementada como `@asset`, permitindo:

- Observabilidade completa
- Materialização explícita
- Reexecução seletiva
- Controle de dependências

## ✔ Controle Transacional

- Conexão manual com PostgreSQL
- Commit explícito
- Encerramento seguro de recursos
- Idempotência estrutural

## ✔ Estratégia Arquitetural

- Separação clara entre ingestão e transformação
- Camadas independentes
- Preparação para expansão futura
- Estrutura cloud-ready

---

# 🚀 Benefícios Arquiteturais

- Pipeline modular
- Separação de responsabilidades
- Governança clara
- Estrutura preparada para BI
- Base para modelagem dimensional
- Pronto para evolução para Data Platform

---

# 🔮 Evolução Natural do Projeto

Próximos passos arquiteturais recomendados:

- Implementação de surrogate keys
- Separação formal entre fato e dimensão
- Implementação de SCD Type 2
- Particionamento de tabelas
- Implementação de carga incremental
- Integração com Data Lake
- Deploy em ambiente cloud (Azure / AWS)

---

# 👨‍💻 Autor

Walney de Negreiros Gomes  
Data Engineer 
- Modelagem Analítica
- Orquestração de Pipelines
