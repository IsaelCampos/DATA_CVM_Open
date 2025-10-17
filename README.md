# Análise Fundamentalista CVM (DFP) – DRE & BPP com indicadores e Excel

> CLI que busca empresas na base DFP/BPP, alinha anos DRE×BPP, calcula séries históricas (ROE, ROA, margens, endividamento, liquidez), e exporta Excel com abas resumo, dados, DRE_full, BPP_full, indicadores_hist, contas_hist e gráfico de Receita vs Lucro.

`Instalação` 
```txt
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
````

`Uso`
```txt
# Buscar empresa
python app.py search --q "Banco do Brasil" --exact --ini 2018 --fim 2025

# Analisar e exportar Excel
python app.py analyze --empresa "Banco do Brasil" --exact --ini 2018 --fim 2025 --out resultado.xlsx
# ou por CNPJ
python app.py analyze --cnpj 00000000000191 --ini 2018 --fim 2025 --out resultado.xlsx

```
