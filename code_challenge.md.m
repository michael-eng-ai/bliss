# Code Challenge: Data Engineer

## Contexto

Somos uma empresa que está transformando dados em insights para melhorar a experiência do cliente e otimizar operações. Estamos construindo uma plataforma robusta que integra dados de diversas fontes e permite a análise eficiente e confiável desses dados. Sua tarefa é implementar um pipeline de ETL que processe dados e os torne acessíveis para consulta, análise e visualização.

---

## Fontes de Dados

Você receberá dois conjuntos de dados:

- **Transações**: Representa operações entre a empresa e seus clientes, incluindo valores transacionados e condições.
- **Pagamentos**: Representa os pagamentos realizados por clientes em relação às transações previamente registradas.

Esses arquivos podem ser baixados nos seguintes links:

- [transactions.json](transactions.json)
- [payments.json](payments.json)

---

## Definições

- **Transação**: Uma operação comercial que gera obrigações financeiras, como uma compra ou contrato de serviço.
- **Pagamento**: Um registro de que uma obrigação financeira foi quitada.

---

## Desafio ETL

Como uma empresa orientada por dados, é crucial termos uma infraestrutura que permita o armazenamento, processamento e análise dos dados de maneira eficiente e escalável.

Sua tarefa é construir um pipeline de ETL (Extract, Transform, Load) que:

1. **Extraia** os dados dos arquivos fornecidos, tratando-os de forma adequada;
2. **Transforme** os dados aplicando regras de negócio, agregações e tratamento adequado (limpeza, conversões e enriquecimento);
3. **Carregue** os dados em um formato estruturado, armazenado de forma que possa ser consultado via SQL.

A solução deve ser escalável e preparada para lidar com um aumento significativo no volume de dados no futuro.

### Requisitos

- Implementar o pipeline de ETL.
- Os dados processados devem ser armazenados de forma que possam ser consultados via SQL (por exemplo, através de tabelas em um data lake ou banco de dados compatível).
- A solução deve tratar os dados de forma segura, garantindo que não haja perda de informações.
- A implementação deve contemplar o armazenamento de resultados utilizando arquivos Parquet, particionados por um critério que identifique a inadimplência (por exemplo, `inadimplente = true` ou `false`).

### Diferencial

- Implementar o pipeline de ETL utilizando **Apache Spark** (Scala ou Python).
- Emule uma solução de **streaming** utilizando os arquivos fornecidos como fonte de dados.

---

## Governança e Privacidade

### Desafio

Além de processar e armazenar os dados, é fundamental que a solução implemente políticas de governança e privacidade:

- **Governança**:  
  - Descreva e, se possível, implemente uma solução de metadados que controle quem pode acessar quais dados.  
  - Explique como você faria o gerenciamento de permissões e o controle de acesso aos dados processados.

- **Privacidade**:  
  - Mostre como garantir que os dados sensíveis sejam protegidos, incluindo abordagens para anonimização e/ou criptografia dos dados.
  - Caso opte por implementar, integre essa solução ao pipeline ETL.

### Diferencial

- Implemente no pipeline uma etapa voltada para a governança e privacidade, acompanhada de uma documentação que explique as escolhas e estratégias.

---

## Regras de Negócio

### Contexto

Queremos analisar o comportamento dos clientes em relação a pagamentos atrasados. Para isso, estabelecemos dois critérios para classificar um cliente como inadimplente:

- **Critério 1**: O cliente atrasou um pagamento em mais de 30 dias, após um período de 3 meses.
- **Critério 2**: O cliente teve algum atraso superior a 15 dias em um período de 6 meses.

### Sua Tarefa

- Implemente uma lógica que avalie os clientes com base nesses critérios para um período de 6 meses.  
- Forneça os seguintes resultados:
  - A porcentagem de clientes inadimplentes.
  - A lista de clientes considerados inadimplentes, detalhando os respectivos pagamentos que violaram os critérios.
- Armazene os resultados como arquivos Parquet, particionados por um campo que indique se o cliente é inadimplente (`inadimplente = true` ou `false`).

### Requisitos

- A solução deve ser confiável e preparada para escalabilidade em volumes maiores de dados.

### Diferencial

- Implemente o pipeline de ETL utilizando **Apache Spark** (Scala ou Python).

---

## Critérios de Avaliação

- **Eficiência**:  
  - Avaliaremos o tempo de execução do pipeline e o uso de recursos, principalmente em operações distribuídas com Apache Spark.
  
- **Clareza do Código**:  
  - O código deve ser claro, com nomeação adequada de variáveis, funções e módulos.
  
- **Documentação**:  
  - A presença de um README.md que detalhe as instruções de configuração, instalação, execução e explicação das decisões de design.  
  - Uso de docstrings (seguindo o PEP 257) e comentários esclarecedores sobre as escolhas técnicas.

- **Escalabilidade**:  
  - A solução deve ser capaz de lidar com um aumento no volume de dados e demonstrar boas práticas para operações distribuídas.

- **Testes**:  
  - A inclusão de testes unitários (ou de integração) para validar a funcionalidade do pipeline e as regras de negócio será considerada um diferencial.

---

## Entrega

- **Repositório Git**:  
  - A solução deve ser entregue através de um repositório Git. Faça um fork deste repositório e envie um pull request com a solução.
  
- **Prazo**:  
  - O prazo para a entrega da solução é de 7 dias a partir do recebimento do desafio.
  
- **Instruções de Execução**:  
  - Inclua um arquivo `README.md` atualizado com instruções claras sobre como configurar o ambiente, instalar as dependências e executar a solução (incluindo exemplos de comandos, como `spark-submit spark_pipeline.py` e instruções de agendamento ou execução automatizada se implementadas).