CREATE OR REPLACE TABLE `ps-eng-dados-ds3x.janielc895.icf_icc_refined` AS
WITH icc_icf_data AS (
  SELECT
    'ICC' AS origem,
    categoria,
    ROUND(dezembro_23, 2) AS dezembro_23,
    ROUND(janeiro_24, 2) AS janeiro_24,
    ROUND(fevereiro_24, 2) AS fevereiro_24,
    ROUND(marco_24, 2) AS marco_24,
    ROUND(abril_24, 2) AS abril_24,
    ROUND(maio_24, 2) AS maio_24,
    ROUND(junho_24, 2) AS junho_24,
    ROUND(julho_24, 2) AS julho_24,
    ROUND(agosto_24, 2) AS agosto_24,
    ROUND(setembro_24, 2) AS setembro_24,
    ROUND(outubro_24, 2) AS outubro_24,
    ROUND(novembro_24, 2) AS novembro_24,
    ROUND(dezembro_24, 2) AS dezembro_24
  FROM
    `ps-eng-dados-ds3x.janielc895.icc_trusted`
  WHERE
    categoria IN ('Índice de Confiança do Consumidor', 'Índice das Condições Econômicas Atuais', 'Índice de Expectativas do Consumidor')

  UNION ALL

  SELECT
    'ICF' AS origem,
    categoria,
    ROUND(dezembro_23, 2) AS dezembro_23,
    ROUND(janeiro_24, 2) AS janeiro_24,
    ROUND(fevereiro_24, 2) AS fevereiro_24,
    ROUND(marco_24, 2) AS marco_24,
    ROUND(abril_24, 2) AS abril_24,
    ROUND(maio_24, 2) AS maio_24,
    ROUND(junho_24, 2) AS junho_24,
    ROUND(julho_24, 2) AS julho_24,
    ROUND(agosto_24, 2) AS agosto_24,
    ROUND(setembro_24, 2) AS setembro_24,
    ROUND(outubro_24, 2) AS outubro_24,
    ROUND(novenbro_24, 2) AS novembro_24,
    ROUND(dezembro_24, 2) AS dezembro_24
  FROM
    `ps-eng-dados-ds3x.janielc895.icf_trusted`
  WHERE
    categoria IN ('ICF')
)

SELECT
  origem,
  categoria,
  dezembro_23,
  janeiro_24,
  fevereiro_24,
  marco_24,
  abril_24,
  maio_24,
  junho_24,
  julho_24,
  agosto_24,
  setembro_24,
  outubro_24,
  novembro_24,
  dezembro_24,
  -- Calcule a variação percentual entre dezembro_23 e janeiro_24
  ROUND((janeiro_24 - dezembro_23) / dezembro_23 * 100, 2) AS variacao_dez_23_jan_24,
  -- Calcule a variação percentual entre janeiro_24 e fevereiro_24
  ROUND((fevereiro_24 - janeiro_24) / janeiro_24 * 100, 2) AS variacao_jan_24_fev_24,
  -- Calcule a variação percentual entre fevereiro_24 e marco_24
  ROUND((marco_24 - fevereiro_24) / fevereiro_24 * 100, 2) AS variacao_fev_24_marco_24,
  -- Calcule a variação percentual entre marco_24 e abril_24
  ROUND((abril_24 - marco_24) / marco_24 * 100, 2) AS variacao_marco_24_abr_24,
  -- Calcule a variação percentual entre abril_24 e maio_24
  ROUND((maio_24 - abril_24) / abril_24 * 100, 2) AS variacao_abr_24_mai_24,
  -- Calcule a variação percentual entre maio_24 e junho_24
  ROUND((junho_24 - maio_24) / maio_24 * 100, 2) AS variacao_mai_24_jun_24,
  -- Calcule a variação percentual entre junho_24 e julho_24
  ROUND((julho_24 - junho_24) / junho_24 * 100, 2) AS variacao_jun_24_jul_24,
  -- Calcule a variação percentual entre julho_24 e agosto_24
  ROUND((agosto_24 - julho_24) / julho_24 * 100, 2) AS variacao_jul_24_ago_24,
  -- Calcule a variação percentual entre agosto_24 e setembro_24
  ROUND((setembro_24 - agosto_24) / agosto_24 * 100, 2) AS variacao_ago_24_set_24,
  -- Calcule a variação percentual entre setembro_24 e outubro_24
  ROUND((outubro_24 - setembro_24) / setembro_24 * 100, 2) AS variacao_set_24_out_24,
  -- Calcule a variação percentual entre outubro_24 e novembro_24
  ROUND((novembro_24 - outubro_24) / outubro_24 * 100, 2) AS variacao_out_24_nov_24,
  -- Calcule a variação percentual entre novembro_24 e dezembro_24
  ROUND((dezembro_24 - novembro_24) / novembro_24 * 100, 2) AS variacao_nov_24_dez_24
FROM
  icc_icf_data;
