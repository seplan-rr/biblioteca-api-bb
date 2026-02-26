# Biblioteca API BB

*Wrapper* da API do Banco do Brasil.

## Como instalar?

Se você usar o `uv`:

```sh
uv add api_bb
```

Se você usar o `pip`:

```sh
pip install api_bb
```

## Funcionalidades

- Lê os parâmetros `app_key`, `client_id` e `client_secret` das variáveis de
  ambiente `BB_API_APP_KEY`, `BB_API_CLIENT_ID` e `BB_API_CLIENT_SECRET`,
  respectivamente, caso não sejam passadas na instanciação das classes
- Gera o token de acesso automaticamente, gerando um novo a cada 10 minutos,
  tempo de expiração do token definido pelo Banco do Brasil
- Separa operaçãos disponíveis aos órgãos de repasse e de controle em classes
  separadas, mantendo as operações comuns aos dois
- Retorna os resultados das chamadas às APIS em formato de `DataFrame` do
  [`pandas`][pandas]
- Aceita parâmetros em múltiplos formatos, como:
  - CNPJ e CEP podem estar pontuados ou não
  - Datas podem estar em formato `str`, `date` ou `datetime`

## Referências

Os documentos utilizados de referência para criação dessa API foram:

- [Portal Developers BB]
- [Documentação Swagger API BB]

[pandas]: https://pandas.pydata.org/
[Portal Developers BB]: https://apoio.developers.bb.com.br/referency/post/641877548600960012b32cd6
[Documentação Swagger API BB]: https://api.bb.com.br/accountability/v3/swagger
