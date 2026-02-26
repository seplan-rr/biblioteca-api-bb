# Biblioteca API BB

*Wrapper* da API do Banco do Brasil.

## Funcionalidades

- Gera o token de acesso automaticamente, gerando um novo a cada 10 minutos,
  tempo de expiração do token definido pelo Banco do Brasil
- Separa operaçãos disponíveis aos órgãos de repasse e de controle em classes
  separadas, mantendo as operações comuns aos dois
- Retorna os resultados das chamadas às APIS em formato de `DataFrame` do
  `pandas`
- Aceita parâmetros em múltiplos formatos, como:
  - CNPJ e CEP podem estar pontuados ou não
  - Datas podem estar em formato `str`, `date` ou `datetime`

## Referências

Os documentos utilizados de referência para criação dessa API foram:

- [Portal Developers BB]
- [Documentação Swagger API BB]

[Portal Developers BB]: https://apoio.developers.bb.com.br/referency/post/641877548600960012b32cd6
[Documentação Swagger API BB]: https://api.bb.com.br/accountability/v3/swagger
