/**verificas os da coluna icf_raw pra ver se tá tudo OK*/
SELECT * FROM `ps-eng-dados-ds3x.janielc895.icf_raw`
LIMIT 10;


/**criar a tabela icf_trusted e copia os dados da tabela icf é já eliminar as colunas nula**/
CREATE OR REPLACE TABLE ps-eng-dados-ds3x.janielc895.icf_trusted AS
SELECT * 
FROM ps-eng-dados-ds3x.janielc895.icf_raw
WHERE categoria != 'Fonte: FecomercioSP';


/*verificar se todos os dados foram copiados de acordo com o codigo acima*/
SELECT * 
FROM ps-eng-dados-ds3x.janielc895.icf_trusted
LIMIT 5;
