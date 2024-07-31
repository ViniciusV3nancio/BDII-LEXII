/* Essa Lista deu um belo trabalho Professor, tive ajuda do Caio, Heitor e Luis para fazer e mesmo assim acho que fiz coisa errada
Espero que o sr n�o pese muito a m�o na hora de dar a nota kkk, boas f�rias pra gente Pedr�o!
*/

-- Parte 1 --

-- 1. Criando o Banco de Dados -- 
Create Database ListaDeExerciciosII
Go

-- 2. Entrando no Banco -- 
Use ListaDeExerciciosII
Go

-- 3. Alterar o modelo de recupera��o para Bulk_Logged -- 
Alter Database ListaDeExerciciosII
Set Recovery Bulk_Logged
Go

-- 4. Importa��o Realizada -- 

-- 5. Definindo a Coluna Chave Prim�ria Auto Numerada -- 
Alter Table [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Add CodigoQueimadas2023 Int Primary Key  Identity (1,1)
Go

-- 6. Alterando o Tipo de dados da coluna DataHora para DateTime -- 
Alter Table[Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Alter Column [DataHora] DateTime
Go

-- 7. Realizando a Cria��o de um novo indice NonClustered para a coluna DataHora -- 
Create NonClustered Index Ind_Datahora On [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023] (DataHora)
Go

-- 8. Realizando a cria��o de um novo indice NonClustered para a coluna Bioma -- 
Create NonClustered Index Ind_Bioma On [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023] (Bioma)
Go

-- 9. Realize a Cria��o de uma nova estatistica para coluna DiaSemChuva -- 
Create Statistics StatisticsDiaSemChuva 
On [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023] (DiaSemChuva)
Go

DBCC Show_Statistics ('[Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]', StatisticsDiaSemChuva)
Go

-- Parte 2 -- 

-- 1. Criando a View V_VisaoMesEstadoMunicipioBioma -- 
Create View  V_VisaoMesEstadoMunicipioBioma
As 
Select Month(DataHora) As Mes,
	   Estado,
	   Municipio,
	   Bioma,
	   Count (*) As TotalDeQueimadas
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Month(DataHora) In (2,4,6,8,10,12)
Group By Month(DataHora), Estado, Municipio, Bioma
Go

-- Verificando a View -- 
Select Mes, Estado, Municipio, Bioma, TotalDeQueimadas
From V_VisaoMesEstadoMunicipioBioma
Go

-- 2. Criando a View V_DuzentasPrimeirasQueimadas -- 
Create View V_DuzentasPrimeirasQueimadas
As 
Select 
Estado,
Municipio,
Bioma,
Convert(Varchar, First_Value(DataHora) 
Over 
(Partition By Estado, Municipio, Bioma Order By DataHora), 103) As [Primeira Queimada Ocorrida],
Convert(Varchar, Last_Value(DataHora) 
Over 
(Partition By Estado, Municipio, Bioma Order By DataHora Rows Between Unbounded Preceding 
And Unbounded Following ), 103) As [�ltima Queimada Ocorrida]
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Go

-- Verificando a View -- 
Select Estado, Municipio, Bioma, [Primeira Queimada Ocorrida], [�ltima Queimada Ocorrida]
From V_DuzentasPrimeirasQueimadas
Go

-- 3. Criando a CTERanqueamento -- 
With CTERanqueamento 
As
(Select
Row_Number() Over (Order By Estado, Municipio, Bioma) As Ranking,
Estado, Municipio, Bioma 
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
)
Select Ranking, Estado, Municipio, Bioma From CTERanqueamento
Go

-- 4. Alterando a CTERanqueamento e adicionando a Coluna Quantidade -- 
With CTERanqueamento 
As
(Select
Row_Number() Over (Order By Estado, Municipio, Bioma) As Ranking,
Estado, Municipio, Bioma,
(Select Count (*) From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]) As Quantidade
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
)
Select Ranking, Estado, Municipio, Bioma, Quantidade From CTERanqueamento
Go

-- 5. Modificando a Parti��o de Ranqueamento de Dados da CTERanqueamento -- 
With CTERanqueamento 
As
(Select
Row_Number() Over (Partition By Count(DataHora) Order By Estado, Municipio, Bioma) As Ranking,
Estado, Municipio, Bioma,
Count (*) As Quantidade From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Group By Estado, Municipio, Bioma
)
Select Ranking, Estado, Municipio, Bioma, Quantidade 
From CTERanqueamento
Go

-- Parte 3 - Desenvolvimento da Clausula Output -- 

-- 1. Criando a Tabela HistoricoQueimadas2023 com a mesma estrutura da tabela Queimadas2023 -- 
Create Table HistoricoQueimadas2023
(DataHora DateTime Null,
Satelite nVarChar(50) Not Null,
Pais nVarChar(50) Not Null,
Estado nVarChar(50) Not Null,
Municipio nVarChar(50) Not Null,
Bioma nVarChar(50) Not Null,
DiaSemChuva TinyInt Not Null,
Precipitacao Float Not Null,
RiscoFogo Float Not Null,
Latitude Float Not Null,
Manipulacao nVarChar (50))
Go


-- 2. Realizando o Update de 10 Registros na tabela de Queimadas2023 com o Valor de DataHora '14/05/2023' --

-- Inserindo os dados na tabela e HistoricosQueimadas2023 -- 
Insert Into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'Update'
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Convert(Varchar, DataHora, 23) = '2023-05-14'
Order By Satelite Desc -- Pensei que teria 10 linhas de registro, mas era uma linha com 10 registros kk -- 
Go

-- Fazendo a altera��o de estado e municipio -- 
Update [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Set Estado = 'Pernambuco', Municipio = 'Jaboat�o dos Guararapes'
Where Convert(Varchar, DataHora, 23) = '2023-05-14'
Go

Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Convert(Varchar, DataHora, 23) = '2023-05-14'
Go

-- 3. Inserindo os Dados que ser�o apagados da coluna CodigoQueimadas -- 
Insert Into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
Select Top 10 DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'Delete'
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Estado = 'Minas Gerais'
Order By RiscoFogo Asc
Go

-- Deletando os 10 Primeiros Registros do Estado de Minas Gerais Filtrados pelo Risco Fogo Asc -- 
With CTEDelete
As 
(Select Top 10 DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Estado = 'Minas Gerais'
Order By RiscoFogo Asc
)
Delete From CTEDelete
Go

-- Verificando as Linhas de Registro que foram inseridas na Tabela de HistoricoQueimadas2023 -- 
Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao
From HistoricoQueimadas2023
Where Manipulacao = 'Delete'
Go

-- Verificando as Linhas que restaram na tabela Queimadas2023 com o Estado de Minas Gerais -- 
Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, CodigoQueimadas2023
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where Estado = 'Minas Gerais'
Order By RiscoFogo Asc
Go

-- 4. Inserindo 20 Linhas de Registro na Tabela de Queimadas2023 -- 
Insert Into [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
(DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude)
Values
('2024-06-01T08:00:00', 'GOES-16', 'Brasil', 'S�o Paulo', 'S�o Paulo', 'Mata Atl�ntica', 3, 12.5, 0, -23.5505),
('2024-06-02T09:00:00', 'Himawari-8', 'Brasil', 'Minas Gerais', 'Belo Horizonte', 'Cerrado', 5, 8.2, 0, -19.9167),
('2024-06-03T10:00:00', 'Meteosat-11', 'Brasil', 'Bahia', 'Salvador', 'Caatinga', 1, 2.0, 0, -12.9714),
('2024-06-04T11:00:00', 'GOES-17', 'Brasil', 'Rio de Janeiro', 'Rio de Janeiro', 'Mata Atl�ntica', 2, 6.8, 0, -22.9068),
('2024-06-05T12:00:00', 'Meteosat-10', 'Brasil', 'Paran�', 'Curitiba', 'Mata Atl�ntica', 4, 15.3, 0, -25.4284),
('2024-06-06T13:00:00', 'Himawari-8', 'Brasil', 'Amazonas', 'Manaus', 'Amaz�nia', 6, 3.5, 0, -3.1190),
('2024-06-07T14:00:00', 'GOES-16', 'Brasil', 'Pernambuco', 'Recife', 'Mata Atl�ntica', 0, 0.0, 0, -8.0476),
('2024-06-08T15:00:00', 'Meteosat-11', 'Brasil', 'Rio Grande do Sul', 'Porto Alegre', 'Pampa', 8, 21.0, 0, -30.0330),
('2024-06-09T16:00:00', 'GOES-17', 'Brasil', 'Cear�', 'Fortaleza', 'Caatinga', 9, 7.1, 0, -3.7172),
('2024-06-10T17:00:00', 'Himawari-8', 'Brasil', 'Par�', 'Bel�m', 'Amaz�nia', 2, 4.9, 0, -1.4558),
('2024-06-11T08:00:00', 'GOES-16', 'Brasil', 'Goi�s', 'Goi�nia', 'Cerrado', 1, 10.0, 0, -16.6869),
('2024-06-12T09:00:00', 'Himawari-8', 'Brasil', 'Mato Grosso', 'Cuiab�', 'Pantanal', 3, 5.6, 0, -15.6010),
('2024-06-13T10:00:00', 'Meteosat-11', 'Brasil', 'Santa Catarina', 'Florian�polis', 'Mata Atl�ntica', 4, 11.7, 0, -27.5954),
('2024-06-14T11:00:00', 'GOES-17', 'Brasil', 'Esp�rito Santo', 'Vit�ria', 'Mata Atl�ntica', 7, 6.3, 0, -20.3155),
('2024-06-15T12:00:00', 'Meteosat-10', 'Brasil', 'Tocantins', 'Palmas', 'Cerrado', 2, 9.0, 0, -10.2491),
('2024-06-16T13:00:00', 'Himawari-8', 'Brasil', 'Maranh�o', 'S�o Lu�s', 'Amaz�nia', 3, 7.8, 0, -2.5307),
('2024-06-17T14:00:00', 'GOES-16', 'Brasil', 'Alagoas', 'Macei�', 'Mata Atl�ntica', 5, 13.1, 0, -9.6498),
('2024-06-18T15:00:00', 'Meteosat-11', 'Brasil', 'Rond�nia', 'Porto Velho', 'Amaz�nia', 6, 4.3, 0, -8.7608),
('2024-06-19T16:00:00', 'GOES-17', 'Brasil', 'Para�ba', 'Jo�o Pessoa', 'Mata Atl�ntica', 1, 2.5, 0, -7.1150),
('2024-06-20T17:00:00', 'Himawari-8', 'Brasil', 'Amap�', 'Macap�', 'Amaz�nia', 4, 6.0, 0, 0.0346)
Go

-- Inserindo os Dados na Tabela de HistoricoQueimadas -- 
Insert Into HistoricoQueimadas2023 (DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao)
Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, 'Insert'
From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
Where DataHora Between '2024-06-01T08:00:00' And '2024-06-20T17:00:00'
Go

-- Verificando se os Dados foram Inseridos Corretamente na Tabela de Hist�ricoQueimadas2023 -- 
Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude, Manipulacao
From HistoricoQueimadas2023
Where Manipulacao = 'Insert'
Go

-- Parte 4 - Desenvolvimento de Stored Procedure e User Defined Functions -- 

-- 1. Criando a SP P_FiltrarMesesQueimadas -- 
Create Procedure P_FiltrarMesesQueimadas
 @Mes TinyInt
 As
 Begin
 Select Estado, Municipio, DataHora, Latitude
 From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
 Where Month(Datahora) = @Mes
 Order By DataHora Desc
 End
Go

 -- Consultando a SP P_FiltrarMesesQueimadas -- 
 Exec P_FiltrarMesesQueimadas @Mes = 12
 Go

 -- 2. Criando a SP P_FiltrarLocalQueimada -- 
Create Procedure P_FiltrarLocalQueimada
 @LocalDaOcorrencia nVarChar(50),
 @Local nVarChar(50)
 As
 Begin
	If @Local = 'Estado'
	Begin Select Estado As 'Local da Ocorr�ncia', Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Estado = @LocalDaOcorrencia
	Order By DataHora Desc
	End
	Else If @Local = 'Municipio'
	Begin Select Municipio As 'Local da Ocorr�ncia', Estado, Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Municipio = @LocalDaOcorrencia
	Order By DataHora Desc
	End
End
Go

-- Testando a SP pelo Estado -- 
Exec P_FiltrarLocalQueimada @LocalDaOcorrencia = 'Mato Grosso', @Local = 'Estado'
Go

-- Testando a SP pelo Municipio -- 
Exec P_FiltrarLocalQueimada @LocalDaOcorrencia = 'Curitiba', @Local = 'Municipio'
Go

-- 3. Criando a UDF F_PesquisarLatitude --  // Imagino que seja pela Latitude pq n�o h� longitude nesta base de dados

Create Function F_PesquisarLatitude
(
	@Latitude float
)
Returns Table
As
Return
(
	Select Estado, Municipio
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]	
	Where Latitude = @Latitude
)
Go

-- Verificando se a Fun��o Puxa alguma coisa -- 
Select Estado, Municipio
From F_PesquisarLatitude (-7.1150)
Go

-- 4. Criando a UDF F_PesquisarBioma para buscar o Bioma -- 
Create Function F_PesquisarBioma
(
	@LocalDaOcorr�ncia nVarChar(50),
	@Local nVarChar(50)
)
Returns Table
As
Return
(
	Select Bioma, Estado, Municipio
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where(@LocalDaOcorr�ncia = 'Estado' And Estado = @Local)
	Or (@LocalDaOcorr�ncia = 'Municipio' And Municipio = @Local)
)
Go

-- Verificando se a UDF est� funcionando Por Municipio -- 
Select Estado, Municipio, Bioma From F_PesquisarBioma('Municipio', 'Jaboat�o dos Guararapes')
Go

-- Verificando se a UDF est� funcionando Por Estado --
Select Estado, Municipio, Bioma From F_PesquisarBioma('Estado', 'Rio de Janeiro')
Go

-- Parte 5 Desenvolvimento - Tratamento de Erros e Performance -- 

-- 1. Adicionando Coment�rios em Ambas as as SP P_FiltrarQueimadas e  P_FiltrarLocalQueimada -- 

Create Procedure P_FiltrarMesesQueimadas
 @Mes TinyInt -- Definindo o M�s em que voc� gostaria de buscar a Queimada na Tabela de Queimadas2023 -- 
 As
 Begin
 Select Estado, Municipio, DataHora, Latitude
 From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
 Where Month(Datahora) = @Mes
 Order By DataHora Desc 
 End
Go

Create Or Alter Procedure P_FiltrarLocalQueimada
 @LocalDaOcorrencia nVarChar(50), -- Definindo Primeiro se ser� em um Estado ou Municipio -- 
 @Local nVarChar(50) -- Ap�s escolher na vari�vel interior escolher o local que ocorreu a queimada -- 
 As
 Begin
	If @Local = 'Estado'
	Begin Select Estado As 'Local da Ocorr�ncia', Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Estado = @LocalDaOcorrencia
	Order By DataHora Desc
	End
	Else If @Local = 'Municipio'
	Begin Select Municipio As 'Local da Ocorr�ncia', Estado, Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Municipio = @LocalDaOcorrencia
	Order By DataHora Desc
	End
End
Go

-- 2. Adicionando Valores Padr�es de inicializa��o em Ambas as SP P_FiltrarQueimadas e  P_FiltrarLocalQueimada -- 

Create Or Alter Procedure P_FiltrarMesesQueimadas
 @Mes TinyInt -- Definindo o M�s em que voc� gostaria de buscar a Queimada na Tabela de Queimadas2023 -- 
 As
 Set NoCount On -- 4. Diretiva para Desativar a Contagem de Linhas -- 
 Set Language Brazilian -- 3. Diretiva de Idioma PT-BR -- 
 Set Ansi_Warnings On -- 5. Diretiva de Desativar a Apresenta��o de Alertas - Warnings --
 Begin
 Set @Mes = (Select Case When @Mes Is Null Then Month(GetDate())
				Else @Mes
				End) -- Utilizando um Valor Padr�o Caso o Valor da Vari�vel @Mes n�o seja inserida --
 Select Estado, Municipio, DataHora, Latitude
 From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
 Where Month(Datahora) = @Mes
 Order By DataHora Desc 
 End
Go

Create Or Alter Procedure P_FiltrarLocalQueimada
 @LocalDaOcorrencia nVarChar(50), -- Definindo Primeiro se ser� em um Estado ou Municipio -- 
 @Local nVarChar(50) -- Ap�s escolher na vari�vel interior escolher o local que ocorreu a queimada -- 
 As
 Set NoCount On -- 4. Diretiva para Desativar a Contagem de Linhas -- 
 Set Language Brazilian -- 3. Diretiva de Idioma PT-BR -- 
 Set Ansi_Warnings On -- 5. Diretiva de Desativar a Apresenta��o de Alertas - Warnings -- 
 Begin
 If (@LocalDaOcorrencia Is Not Null And @Local Is Not Null) -- Inserindo Valores Padr�es serem obrigat�rios para a Procedura Funcionar -- 
 Begin Try -- 6. Adicionando a T�cnica Begin Try para Tratamento de Erros -- 
	If @Local = 'Estado'
	Begin Select Estado As 'Local da Ocorr�ncia', Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Estado = @LocalDaOcorrencia
	Order By DataHora Desc
	End
	Else If @Local = 'Municipio'
	Begin Select Municipio As 'Local da Ocorr�ncia', Estado, Bioma, DataHora As 'Data da Ocorr�ncia'
	From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
	Where Municipio = @LocalDaOcorrencia
	Order By DataHora Desc
	End 
End Try
  Begin Catch  -- 6. Adicionando a T�cnica Begin Catch para Tratamento de Erros -- 
 -- 7. Adicionando as Fun��es Respons�veis em identificar o n�mero de Linha em que apresenta o erro, mensagem de erro, e n�vel de severidade -- 
   SELECT ERROR_NUMBER() AS ErrorNumber,
          ERROR_SEVERITY() AS ErrorSeverity,
          ERROR_STATE() AS ErrorState,
          ERROR_PROCEDURE() AS ErrorProcedure,
          ERROR_MESSAGE() AS ErrorMessage,
          ERROR_LINE() AS ErrorLine;         
  End Catch -- Encerrando o Begin Catch --   
  End
  Go

  -- Imagino que n�o seja para usar a diretiva de Arithabort pois, n�o tem erros aritm�ticos para serem abortados -- 

  

  Create View P_BuscaQueimadas2023 
  With Encryption -- 8. Aplicando a Criptografia de Codigo Fonte -- 
  As
  Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude
  From [Banco de Dados II � Lista de Exerc�cios II � Queimadas 2023]
  Go

  Select DataHora, Satelite, Pais, Estado, Municipio, Bioma, DiaSemChuva, Precipitacao, RiscoFogo, Latitude
  From P_BuscaQueimadas2023
  Go















