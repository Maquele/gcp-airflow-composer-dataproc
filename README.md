Esta DAG tem propósito didático e executa as seguintes ações:
1- [START cluster] Cria um cluster no Dataproc
2- [START job] Roda um job no cluster criado
3- [START delete cluster] Deleta o cluster criado
4- [START move gcs] Move os arquivos

A DAG foi criada a partir do seguinte enunciado:
"Por um motivo qualquer e sem razão nenhuma, a empresa Empresa decidiu criar um pipeline que leia um arquivo .txt armazenado no seu bucket. 
Para fazer a leitura desse arquivo, a empresa usará um script PySpark, que, dentro do GCP, será rodado em cluster Dataproc. 
Porém, todo dia, um novo arquivo .txt é carregado no bucket e para que esse job pudesse lê-lo seria necessário executá-lo manualmente todos os dias. Além disso, o cluster de Dataproc para rodar o job ficaria ligado, dependendo de que uma pessoa o desligue. 
A fim de organizar esse pipeline e utilizar boas práticas, foi sugerida a inclusão de um orquestrador."