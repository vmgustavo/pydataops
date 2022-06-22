# pydataops

Contributing to the debate of which is the best data processing library to use
in Python. The ideia is to test only the default implementation of the library,
no specializations implemented. Any library can be use to perform well in many
tasks, this shall evaluate the performance of the library with the lowest effort
necessary.

This shall test:

* multiple data sizes (`Mib`, `Gib`)
* multiple data operations (`join`, `groupby`, `aggregate`)
* multiple data types (`string`, `integer`, `float`)

## Test Hardware

### Google Colab

|            |                                |
|-----------:|:-------------------------------|
|        CPU | Intel(R) Xeon(R) CPU @ 2.20GHz |
|     Memory | ~ 12GB                         |
| Hard Drive | ~ 30GB                         |
|            |                                |
