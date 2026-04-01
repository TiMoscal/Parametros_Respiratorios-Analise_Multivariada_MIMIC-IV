# Parametros_respiratorios-Analise_multivariada_MIMIC-4
Projeto de extensão de avaliação de risco a partir de parâmetros respiratórios coletados no banco de dados MIMIC-4.

# Avaliação de Desfechos Respiratórios Pós-Extubação por Análise Multivariada de Variáveis Clínicas e Sinais Vitais

Este projeto investiga a associação entre variáveis clínicas, sinais vitais e desfechos adversos em pacientes de unidade de terapia intensiva (UTI) após extubação, utilizando análise multivariada de dados provenientes do banco MIMIC-IV.

O objetivo é identificar padrões fisiopatológicos associados ao risco de reintubação e mortalidade, contribuindo para a compreensão dos fatores relacionados a desfechos respiratórios adversos no período pós-extubação.

---

# Contexto Clínico

A monitorização de pacientes críticos após extubação na unidade de terapia intensiva (UTI) ainda representa um importante desafio clínico.

A falha de extubação pode levar à necessidade de reintubação, evento associado a maior risco de complicações e aumento da mortalidade. Nesse contexto, métodos computacionais capazes de analisar múltiplas variáveis fisiológicas simultaneamente podem auxiliar na identificação de padrões associados ao agravamento do estado respiratório.

Este projeto utiliza análise multivariada de dados clínicos e sinais vitais para investigar associações entre parâmetros fisiológicos e a ocorrência de desfechos respiratórios adversos em pacientes críticos.

---

# Objetivo

Investigar retrospectivamente a associação entre variáveis fisiologicamente relevantes e a incidência de reintubação ou óbito em pacientes submetidos à ventilação mecânica, por meio de análise multivariada de dados clínicos e sinais vitais.

---

# Base de Dados

Os dados utilizados neste estudo foram obtidos a partir do banco público:

**MIMIC-IV (Medical Information Mart for Intensive Care IV)**

https://physionet.org/content/mimiciv/

O MIMIC-IV é uma base de dados pública que contém informações clínicas detalhadas de pacientes internados em unidades de terapia intensiva, amplamente utilizada em pesquisas em medicina intensiva e ciência de dados aplicada à saúde.

---

# Workflow do Projeto

O fluxograma abaixo apresenta o pipeline atual de processamento e análise dos dados utilizado neste projeto. Como o estudo ainda está em desenvolvimento, algumas etapas estão sendo refinadas e reorganizadas.

O processo inicia com a seleção de pacientes a partir do banco de dados MIMIC-IV, seguida pela separação das classes de interesse e seleção das variáveis clínicas relevantes.

Após a extração dos dados, é realizada a limpeza dos arquivos CSV, incluindo:

- tratamento de valores faltantes  
- identificação e remoção de outliers  
- normalização temporal dos sinais  
- marcação dos desfechos clínicos  

Na etapa seguinte são conduzidas análises estatísticas e caracterização inicial dos dados, seguidas por análises multivariadas em duas e três dimensões para investigar associações entre variáveis fisiológicas e os desfechos clínicos.

Os resultados dessas análises são organizados em arquivos contendo combinações de pares e trios de variáveis.

<img width="848" height="932" alt="image" src="https://github.com/user-attachments/assets/1c7cca8b-8710-43c4-a1ff-343deef800d8" />


---

# Autores

Projeto desenvolvido por:

- Tiago Sacarrão Moscal  
- Giulia Santana de Mello  
- Giovanna Cerioni Mastrogiuseppe  
- Federico Aletti  
- Felipe Fava de Lima
- Erik B. Kistler  

 

---

# Observação

O banco de dados MIMIC-IV requer certificação e autorização de acesso por meio da plataforma PhysioNet. Este repositório não disponibiliza dados clínicos diretamente, apenas os códigos utilizados para análise.


