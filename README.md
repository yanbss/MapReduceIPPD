# MapReduce
Esta aplicação foi desenvolvida em JAVA e cria um arquivo de Índice Invertido utilizando o Spark. Este programa pode ser executado em um grande conjunto de dados (arquivos texto).

Para os testes desenvolvidos nesta aplicação utilizou-se banco de dados de 250MB até 1GB, os textos foram extraídos do Wikipédia dump e depois utilizou-se o [wikipedia-extractor](https://github.com/bwbaugh/wikipedia-extractor) para separar o arquivo XML em vários arquivos pequenos. Após isso utilizou-se os arquivos do [scripts python](https://github.com/yanbss/MapReduceIPPD/tree/master/scripts%20python) na seguinte ordem:
* [descompacta.py](https://github.com/yanbss/MapReduceIPPD/blob/master/scripts%20python/descompacta.py)
* [renameFiles.py](https://github.com/yanbss/MapReduceIPPD/blob/master/scripts%20python/renameFiles.py)
* [changeFolder.py](https://github.com/yanbss/MapReduceIPPD/blob/master/scripts%20python/changeFolder.py)

Pode ser utilizado qualquer outro conjunto de arquivos textos para a utilização desta aplicação, não sendo necessária a execução dos códigos acima.

Este trabalho foi desenvolvido como trabalho final da disciplina de IPPD na UFPel, pelos alunos:
* [Renata Z. Junges](https://github.com/rejunges)
* [Yan B. Soares](https://https://github.com/yanbss) 
