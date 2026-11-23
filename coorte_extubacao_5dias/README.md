# Coorte de extubação — janela de 5 dias (T=0 na extubação)

Pré-processamento da coorte de pacientes ventilados do MIMIC-IV (IRA e Sepse + IRA) com o tempo zero de cada internação definido na **primeira extubação registrada**. Os pacientes são acompanhados por até 120 h (5 dias) depois da extubação, e o desfecho é reintubação e/ou óbito nessa janela.

## Como rodar

```bash
pip install pandas numpy matplotlib scipy
python coorte_extubacao_5dias/t0_extub5d.py
```

Por padrão o script lê os três arquivos de entrada de `../CBEB-02/` (`pacientes.csv`, `time_events_SEPSE_CBEB.csv.zip`, `resultados_pacientes_CBEB.csv.zip`) e salva tudo em `coorte_extubacao_5dias/resultados/`. Para usar outras pastas: `--dados <pasta> --saida <pasta>`. Os parâmetros (janelas, tolerância entre intubações, variante da Regra 2) ficam no dicionário `CONFIG` no início do script.

## Etapas e números

| Etapa | Critério | Retidos | Excluídos |
|---|---|---:|---:|
| 0 | Base original | 1522 | – |
| 1 | Sem evento de extubação | 1226 | 296 |
| 1b | Mais de uma intubação antes do T=0 (episódios a ≤1 h fundidos) | 1193 | 33 |
| 2 | Regra 1 — óbito ≤12 h após o T=0 | 1077 | 116 |
| 3 | Regra 2 — morte ou reintubação >120 h após o T=0 | 963 | 114 |
| 4 | Regra 3 — remove registros com `Tempo_desde_Extubacao` > 120 h | 963 | 0 pacientes (268.406 resultados e 130.152 eventos removidos) |

## Saídas (`resultados/`)

| Arquivo | Conteúdo |
|---|---|
| `pacientes_10dias.csv` | Coorte final (963): T=0, datas e horas até óbito/reintubação, `desfecho_5dias` |
| `resultados_pacientes_10dias.csv.zip` | Resultados (vitais/exames) da coorte com `Tempo_desde_Extubacao` em horas, truncados em ±120 h |
| `time_events_10dias.csv.zip` | Eventos da coorte com `Tempo_desde_Extubacao` e `Tempo_fim_desde_Extubacao` |
| `auditoria_pacientes_10dias.csv` | Todas as 1522 internações com `status_coorte` e motivo de exclusão |
| `log_exclusoes_10dias.csv` | Pacientes retidos/excluídos em cada etapa |
| `volume_linhas_10dias.csv` | Nº de linhas antes e depois do corte temporal |
| `estatisticas_excluidos_10dias.csv` | Excluídos vs coorte final: diagnóstico, mortalidade, tempo de VM e vitais das primeiras 24 h de UTI (mediana [IQR], p-valores) |
| `graficos/01–09_*.png` | Cascata de pacientes, distribuição das exclusões por regra, perfil de desfechos da coorte, tempo até reintubação, comparação dos excluídos |

Os dois CSVs grandes estão zipados (o script de entrada também aceita `.csv.zip`).
