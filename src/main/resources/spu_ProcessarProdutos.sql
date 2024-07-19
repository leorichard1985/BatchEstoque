

CREATE TABLE tb_TMP_ProdEstoque (
	Id_Produto		INT,
	Nome_Produto	VARCHAR(255),
	Qtde_Disponivel INT)


GO


SELECT * FROM tb_mov_prod_estoque

id_produto	nome_produto				qtde_disponivel
1			Papel Higienico Reforçado	10
2			Papel Higienico II			0


CREATE PROCEDURE dbo.spu_ProcessarProdutos
AS
BEGIN

--ATUALIZA QTDE. ESTOQUE PRODUTOS EXISTENTES
UPDATE A
	SET
		A.nome_produto = B.Nome_Produto,
		A.qtde_disponivel = A.qtde_disponivel + B.Qtde_Disponivel
FROM tb_mov_prod_estoque A
INNER JOIN tb_TMP_ProdEstoque B
	ON 
		A.id_produto = B.Id_Produto

--CRIA ESTOQUE COM NOVOS PRODUTOS
INSERT INTO tb_mov_prod_estoque (nome_produto,qtde_disponivel)
SELECT 
	nome_produto,qtde_disponivel
FROM tb_TMP_ProdEstoque
WHERE id_produto NOT IN (SELECT Id_Produto FROM tb_mov_prod_estoque WITH (NOLOCK))

--LIMPA TMP
TRUNCATE TABLE tb_TMP_ProdEstoque


END
