/*para selecionas todas as linhas para analise*/
SELECT * FROM `ps-eng-dados-ds3x.janielc895.icc_raw`;

/**criar a tabela icc_trusted e copia os dados da tabela icf é já eliminar as colunas nula**/
CREATE OR REPLACE TABLE ps-eng-dados-ds3x.janielc895.icc_trusted AS
SELECT * 
FROM ps-eng-dados-ds3x.janielc895.icc_raw
WHERE categoria != 'Fonte: FecomercioSP';

/*ver os resultados do carregamento dos dados**/
SELECT * FROM `ps-eng-dados-ds3x.janielc895.icc_trusted`;