# pydataops

Contributing to the debate of which is the best data processing library to use
in Python. The ideia is to test only the default implementation of the library,
no specializations implemented. Any library can be use to perform well in many
tasks, this shall evaluate the performance of the library with the lowest effort
necessary.

This shall test:

* multiple data sizes (`1E3`, `1E6`)
* multiple data operations (`join`, `groupby`, `aggregate`)
* multiple data types (`string`, `integer`, `float`)

## Data Processing Libraries

* [x] `pandas`
* [x] `pyspark`
* [x] `polars`
* [x] `modin-dask`
* [x] `modin-ray`
* [x] `vaex`
* [ ] `pyarrow`
* [ ] `koalas`
* [ ] `datatable`
* [ ] `fugue`

## Test Hardware

### Google Colab

|            |                                |
|-----------:|:-------------------------------|
|        CPU | Intel(R) Xeon(R) CPU @ 2.20GHz |
|     Memory | ~ 12GB                         |
| Hard Drive | ~ 30GB                         |
|            |                                |

---

<sub>Project started during the 2022/01 class of Planejamento e Análise de
Experimentos (Planning and Analysis of Experiments) at the Universidade Federal
de Minas Gerais (Federal University of Minas Gerais)</sub>
