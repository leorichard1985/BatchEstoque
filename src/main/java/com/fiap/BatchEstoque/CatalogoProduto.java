package com.fiap.BatchEstoque;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CatalogoProduto {

	private Integer id_Produto;
	private String nome_Produto;
	private Integer qtde_Disponivel;
	
	public Integer getId_Produto() {
		return id_Produto;
	}

	public String getNome_Produto() {
		return nome_Produto;
	}
	
	public Integer getQtde_Disponivel() {
		return qtde_Disponivel;
	}

	public void setId_Produto(Integer id_Produto) {
		this.id_Produto = id_Produto;
	}
	
	public void setNome_Produto(String nome_Produto) {
		this.nome_Produto = nome_Produto;
	}
	
	
	public void setQtde_Disponivel(Integer qtde_Disponivel) {
		this.qtde_Disponivel = qtde_Disponivel;
	}
}
