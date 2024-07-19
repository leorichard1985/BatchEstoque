CREATE TABLE tb_CAD_UsuarioTipo(
	idUsuarioTipo INT,
	usuarioTipo VARCHAR(50)
);

CREATE TABLE tb_CAD_Usuario(
	idUsuario INT,
	idUsuarioTipo INT,
	nomeUsuario VARCHAR(20),
	cpfUsuario VARCHAR(11)		
);


CREATE TABLE tb_Cad_Restaurante (
	idRestaurante INT, 
	nomeRestaurante VARCHAR(250),
	enderecoRestaurante VARCHAR(100),
	cepRestaurante VARCHAR(10),
	cidadeRestaurante VARCHAR(50),
	estadoRestaurante VARCHAR(2),
	tipoCozinha VARCHAR(50),
	horarioFuncionamento VARCHAR(50),
	capacidadeReservaTotal INT,
	capacidadeReservaDisponivel INT
);
	
	
CREATE TABLE tb_CAD_ReservaTipo(
	idReservaTipo INT,
	reservaTipo VARCHAR(50)
);

CREATE TABLE tb_CAD_Reserva(
	idReserva INT,
	idRestaurante INT,
	idUsuario INT,
	idReservaTipo INT,
	dataReserva VARCHAR(8),
	horaReserva VARCHAR(8)	
);

CREATE TABLE tb_CAD_Avaliacao(
	idAvaliacao INT,
	idReserva INT,
	notaAvaliacao INT,
	comentarioAvaliacao VARCHAR(500)
);
