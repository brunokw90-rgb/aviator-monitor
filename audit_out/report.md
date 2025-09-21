# Random Audit - Relatório

## Métricas Básicas
- n: NA
- média: NA
- mediana: NA
- desvio padrão: NA
- min: NA
- max: NA

## Qui-quadrado (uniformidade por bins)
- bins: 60
- χ²: 16006983.010615 | gl: 59 | p-valor: 0.000000

## Runs (Wald–Wolfowitz)
- corte: median
- runs: 135555 | acima: 135647 | abaixo: 134976
- z: 0.935514 | p-valor: 0.349524

## Runs up-and-down (monotonia)
- runs: 180625 | subidas: 134675 | descidas: 135352
- z: 175.550814 | p-valor: 0.000000

## Autocorrelação (ACF)
- lags mostrados (primeiros 10): 1:-0.000, 2:-0.000, 3:-0.000, 4:-0.000, 5:-0.000, 6:-0.000, 7:-0.000, 8:-0.000, 9:-0.000, 10:-0.000
- intervalo de confiança aprox.: ±0.004

## Gráficos
![Histograma](audit_out\plots\histograma.png)
![ACF](audit_out\plots\acf.png)
![QQ-plot](audit_out\plots\qqplot.png)

## Ljung–Box (autocorrelação conjunta)
- lags (m): 24 | Q: 0.000972 | gl: 24 | p-valor: 1.000000

## Jarque–Bera (normalidade)
- JB: 5854387120.398882 | p-valor: 0.000000
- skewness: 501.575023 | kurtosis: 256928.670424

## BDS (dependência não-linear)
- m=2: Z=-1.099745 | p-valor=0.271443 | eps=3377.479544
- m=3: Z=-0.428513 | p-valor=0.668278 | eps=3377.479544
- m=4: Z=-0.484626 | p-valor=0.627942 | eps=3377.479544
- m=5: Z=-0.377632 | p-valor=0.705704 | eps=3377.479544
