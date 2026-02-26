import os
import base64
import requests
import datetime
import pandas as pd
from typing import Tuple
from api_bb import common


class _AccountabilityV3BaseAPI:
    _app_key: str
    _client_id: str
    _api_domain: str
    _access_token: str
    _oauth_domain: str
    _client_secret: str
    _ambiente: common.Ambiente
    _base64_credentials: str
    _last_access_token_request_timestamp: datetime.datetime

    def __init__(
        self,
        ambiente: common.Ambiente = common.Ambiente.HOMOLOGACAO,
        app_key: str = None,
        client_id: str = None,
        client_secret: str = None,
    ):
        """Inicia uma instância do encapsulador da API Accountability V3 do Banco do Brasil.

        Parâmetros
        ----------
        app_key: str
            Chave da aplicação fornecida pelo Banco do Brasil.
        client_id: str
            ID do cliente fornecido pelo Banco do Brasil ou gerada pela API do Banco do Brasil.
        client_secret: str
            Credencial do cliente gerada pela API do Banco do Brasil.

        Para gerar o ``client_id`` e ``client_secret``, siga os passos disponíveis no Portal BB Developers (<https://apoio.developers.bb.com.br/referency/post/641877548600960012b32cd6>).
        """
        self._ambiente = ambiente

        if ambiente == common.Ambiente.DESENVOLVIMENTO:
            self._api_domain = common._dese_api_domain
            self._oauth_domain = common._dese_oauth_domain
        elif ambiente == common.Ambiente.HOMOLOGACAO:
            self._api_domain = common._homo_api_domain
            self._oauth_domain = common._homo_oauth_domain
        elif ambiente == common.Ambiente.HOMOLOGACAO_ALTERNATIVO:
            self._api_domain = common._homo_alt_api_domain
            self._oauth_domain = common._homo_alt_oauth_domain
        elif ambiente == common.Ambiente.PRODUCAO:
            self._api_domain = common._prod_api_domain
            self._oauth_domain = common._prod_oauth_domain

        app_key = os.getenv("BB_API_APP_KEY", app_key)
        if app_key is not None:
            self._app_key = app_key
        else:
            raise ValueError(
                "Uma chave de aplicação inválida foi utilizada. Defina a chave"
                " por meio do parâmetro 'app_key' ou pelo variável de ambiente"
                " 'BB_API_APP_KEY'."
            )

        client_id = os.getenv("BB_API_CLIENT_ID", client_id)
        if client_id is not None:
            self._client_id = client_id
        else:
            raise ValueError(
                "Um ID de cliente inválido foi utilizado. Defina a chave por"
                " meio do parâmetro 'client_id' ou pelo variável de ambiente"
                " 'BB_API_CLIENT_ID'."
            )

        client_secret = os.getenv("BB_API_CLIENT_SECRET", client_secret)
        if client_secret is not None:
            self._client_secret = client_secret
        else:
            raise ValueError(
                "Um segredo de cliente inválido foi utilizado. Defina a chave"
                " por meio do parâmetro 'client_secret' ou pelo variável de"
                "ambiente 'BB_API_CLIENT_SECRET'."
            )

        base64_credentials = (
            base64
            .b64encode(
                f"{client_id}:{client_secret}".encode("utf-8")
            )
            .decode("utf-8")
        )
        self._base64_credentials = base64_credentials
        self._last_access_token_request_timestamp = None

    def _check_and_update_access_token(self) -> str:
        now = datetime.datetime.now()

        is_first_access_token_request = self._last_access_token_request_timestamp is None

        if is_first_access_token_request:
            diff_between_requests = datetime.timedelta(0)
        else:
            diff_between_requests = now - self._last_access_token_request_timestamp

        is_access_token_expired = diff_between_requests > common._time_between_access_token_requests

        if is_first_access_token_request or is_access_token_expired:
            res = requests.request(
                "POST",
                f"{self._oauth_domain}/oauth/token",
                headers={
                    "Content-Type": "application/x-www-form-urlencoded",
                    "Authorization": f"Basic {self._base64_credentials}",
                },
                data={
                    "grant_type": "client_credentials",
                    "scope": "accountability.statements",
                },
            )

            if res.status_code != 200:
                raise Exception(
                    "Não foi possível adquirir as novas credenciais de acesso."
                )

            res = res.json()
            self._access_token = res["access_token"]
            self._last_access_token_request_timestamp = datetime.datetime.now()

    def _get_access_token(self) -> str:
        self._check_and_update_access_token()
        return self._access_token

    def get_agencias_proximas(
        self,
        cnpj: str,
        cep: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        cnpj = common._handle_numeric_string_with_symbols(cnpj)
        cep = common._handle_numeric_string_with_symbols(cep)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/agencias-proximas",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "cnpj": cnpj,
                "cep": cep,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaAgencia",
            insertables=[
                "quantidadeAgencia",
            ],
            rename_dict={
                "quantidadeAgencia": "Quantidade Agências",
                "codigo": "Código",
                "digito": "Dígito",
                "nome": "Nome",
                "cep": "CEP",
                "logradouro": "Logradouro",
                "bairro": "Bairro",
                "municipio": "Munícipio",
                "siglaUF": "Sigla UF",
                "sugerida": "Sugerida",
            },
        )


class AccountabilityV3RepasseAPI(_AccountabilityV3BaseAPI):
    """Representa um encapsulador da API Accountability V3 do Banco do Brasil
    para os órgaos de repasse.

    Esse encapsulador já reutiliza o token de acesso por 10 minutos e gera um
    novo sempre que o atual estiver expirado.
    """

    def __init__(
        self,
        ambiente: common.Ambiente = common.Ambiente.HOMOLOGACAO,
        app_key: str = None,
        client_id: str = None,
        client_secret: str = None,
    ):
        super().__init__(
            ambiente,
            app_key,
            client_id,
            client_secret,
        )

    def get_extrato_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        start_date: common.DateLike,
        end_date: common.DateLike,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        start_date = common._handle_dates(start_date)
        end_date = common._handle_dates(end_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/statements/{branch_code}-{account_number}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "startDate": start_date,
                "endDate": end_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        return common._handle_results(
            res,
            main_list="transactions",
            insertables=[
                "governmentProgramCode",
                "governmentProgramName",
                "governmentSubProgramCode",
                "governmentSubProgramName",
            ],
            explodeables=[
                "expensesDocuments",
            ],
            rename_dict={
                "governmentProgramCode": "Código Programa Governo",
                "governmentProgramName": "Nome Programa Governo",
                "governmentSubProgramCode": "Código SubPrograma Governo",
                "governmentSubProgramName": "Nome SubPrograma Governo",
                "id": "ID Transação",
                "bookingDate": "Data Agendamento",
                "orderIndex": "Índice Ordem",
                "valueDate": "Data Valor",
                "referenceNumber": "Número Referência",
                "value": "Valor",
                "accountBalance": "Saldo Conta",
                "descriptionCode": "Código Descrição",
                "descriptionName": "Nome Descrição",
                "descriptionBatchNumber": "Número Lote Descrição",
                "creditDebitIndicator": "Indicador Crédito Débito",
                "beneficiaryBankIdentifierCode": "Código Identificador Banco Beneficiário",
                "beneficiaryBranchCode": "Código Agência Beneficiário",
                "beneficiaryAccountNumber": "Número Conta Beneficiário",
                "beneficiaryPersonType": "Tipo Pessoa Beneficiário",
                "beneficiaryDocumentId": "ID Documento Beneficiário",
                "beneficiaryName": "Nome Beneficiário",
                "pendingExpenseConciliation": "Conciliação Despesa Pendente",
                "attachedExpenseDocumentIndicator": "Indicador Anexo Documento Despesa",
                "expenseCategoryCode": "Código Categoria Despesa",
                "expenseIdentificationStatus": "Status Identificação Despesa",
                "subTransactionQuantity": "Quantidade Subtransações",
                "bankOrderRuleCode": "Código Ordem Pagamento Banco",
                "bankOrderPurposeCode": "Código Finalidade Ordem Banco",
                "bankOrderPurposeDescription": "Descrição Finalidade Ordem Banco",
                "expenseSequentialNumber": "Número Sequencial Despesa",
                "expensesCategory": "Categoria Despesa",
                "expensesDocuments": "ID Documento Despesa",
            },
        )

    def get_documento_despesas_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        transaction_id: int,
        document_id: int,
        booking_date: common.DateLike,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        access_token = self._get_access_token()
        booking_date = common._handle_dates(booking_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/expenses/{branch_code}-{account_number}/transactions/{transaction_id}/documents/{document_id}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "bookingDate": booking_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        df_issuer = common._handle_results(
            res["issuer"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
            },
        )

        df_recipient = common._handle_results(
            res["recipient"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
                "presenceTypeCode": "Código Tipo Presença",
                "typeConsumerCode": "Código Tipo Consumidor",
            },
        )

        df_document = common._handle_results(
            res["expenseDocument"],
            main_list="items",
            insertables=[
                "accessKey",
                "receiptTypeCode",
                "typeCode",
                "serialCode",
                "number",
                "issueDate",
                "movementDate",
                "itemDeliveryDate",
                "value",
                "operationTypeName",
                "operation",
                "paymentMethod",
                "digitalSignatureCode",
                "pronafAbilityRegistration",
                "timestamp",
                "userId",
                "discountValue",
                "totalDiscountValue",
                "realeaseInstrumentCode",
                "realeaseInstrumentName",
                "realeaseInstrumentDate",
                "additionalInformation",
            ],
            rename_dict={
                "accessKey": "Chave Acesso",
                "receiptTypeCode": "Código Tipo Recibo",
                "typeCode": "Código Tipo",
                "serialCode": "Código Série",
                "number": "Número",
                "issueDate": "Data Emissão",
                "movementDate": "Data Movimentação",
                "itemDeliveryDate": "Data Entrega",
                "value": "Valor",
                "operationTypeName": "Nome Tipo Operação",
                "operation": "Operação",
                "paymentMethod": "Método Pagamento",
                "digitalSignatureCode": "Código Assinatura Digital",
                "pronafAbilityRegistration": "Registro Habilidade Pronaf",
                "timestamp": "Momento",
                "userId": "ID Usuário",
                "discountValue": "Valor Desconto",
                "totalDiscountValue": "Valor Total Desconto",
                "realeaseInstrumentCode": "Código Liberação de Instrumento",
                "realeaseInstrumentName": "Nome Liberação Instrumento",
                "realeaseInstrumentDate": "Data Liberação Instrumento",
                "additionalInformation": "Informação Adicional",
                "description": "Descrição Item",
                "quantity": "Quantidade Item",
                "metric": "Métrica Item",
                "unitValue": "Valor Unitário Item",
                "totalValue": "Valor Total Item",
                "mercosurCommonNameId": "ID Nome Comum Mercosul",
                "itemDiscountValue": "Valor Desconto Item",
            },
        )

        return (
            df_issuer,
            df_recipient,
            df_document,
        )

    def get_documento_despesas_prestacao_contas(
        self,
        branch_code: int,
        account_number: int,
        transaction_id: int,
        subtransaction_id: int,
        document_id: int,
        booking_date: common.DateLike,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        access_token = self._get_access_token()
        booking_date = common._handle_dates(booking_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/expenses/{branch_code}-{account_number}/transactions/{transaction_id}/subTransactions/{subtransaction_id}/documents/{document_id}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "bookingDate": booking_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        df_issuer = common._handle_results(
            res["issuer"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
            },
        )

        df_recipient = common._handle_results(
            res["recipient"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
                "presenceTypeCode": "Código Tipo Presença",
                "typeConsumerCode": "Código Tipo Consumidor",
            },
        )

        df_document = common._handle_results(
            res["expenseDocument"],
            main_list="items",
            insertables=[
                "accessKey",
                "receiptTypeCode",
                "typeCode",
                "serialCode",
                "number",
                "issueDate",
                "movementDate",
                "itemDeliveryDate",
                "value",
                "operationTypeName",
                "operation",
                "paymentMethod",
                "digitalSignatureCode",
                "pronafAbilityRegistration",
                "timestamp",
                "userId",
                "discountValue",
                "totalDiscountValue",
                "realeaseInstrumentCode",
                "realeaseInstrumentName",
                "realeaseInstrumentDate",
                "additionalInformation",
            ],
            rename_dict={
                "accessKey": "Chave Acesso",
                "receiptTypeCode": "Código Tipo Recibo",
                "typeCode": "Código Tipo",
                "serialCode": "Código Série",
                "number": "Número",
                "issueDate": "Data Emissão",
                "movementDate": "Data Movimentação",
                "itemDeliveryDate": "Data Entrega",
                "value": "Valor",
                "operationTypeName": "Nome Tipo Operação",
                "operation": "Operação",
                "paymentMethod": "Método Pagamento",
                "digitalSignatureCode": "Código Assinatura Digital",
                "pronafAbilityRegistration": "Registro Habilidade Pronaf",
                "timestamp": "Momento",
                "userId": "ID Usuário",
                "discountValue": "Valor Desconto",
                "totalDiscountValue": "Valor Total Desconto",
                "realeaseInstrumentCode": "Código Liberação de Instrumento",
                "realeaseInstrumentName": "Nome Liberação Instrumento",
                "realeaseInstrumentDate": "Data Liberação Instrumento",
                "additionalInformation": "Informação Adicional",
                "description": "Descrição Item",
                "quantity": "Quantidade Item",
                "metric": "Métrica Item",
                "unitValue": "Valor Unitário Item",
                "totalValue": "Valor Total Item",
                "mercosurCommonNameId": "ID Nome Comum Mercosul",
                "itemDiscountValue": "Valor Desconto Item",
            },
        )

        return (
            df_issuer,
            df_recipient,
            df_document,
        )

    def get_extrato_subtransacoes_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        id: int,
        id_subtransaction: str = None,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        params = {
            "gw-dev-app-key": self._app_key,
        }

        if id_subtransaction is not None:
            params["idSubtransaction"] = id_subtransaction

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/statements/{branch_code}-{account_number}/debits/{id}/subtransactions",
            headers=common._get_headers(access_token),
            params=params,
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        return common._handle_results(
            res,
            main_list="subtransactions",
            insertables=[
                "governmentProgramCode",
                "governmentProgramName",
                "governmentSubProgramCode",
                "governmentSubProgramName",
            ],
            explodeables=[
                "expensesCategory",
                "expensesDocuments",
            ],
            rename_dict={
                "governmentProgramCode": "Código Programa Governo",
                "governmentProgramName": "Nome Programa Governo",
                "governmentSubProgramCode": "Código SubPrograma Governo",
                "governmentSubProgramName": "Nome SubPrograma Governo",
                "id": "ID",
                "codeSubtransactionState": "Estado Código Subtransação",
                "paymentState": "Estado Pagamento",
                "paymentDate": "Data Pagamento",
                "value": "Valor",
                "beneficiaryBankIdentifierCode": "Código Identificador Banco Beneficiário",
                "beneficiaryBranchCode": "Código Agência Beneficiário",
                "beneficiaryAccountNumber": "Número Conta Beneficiário",
                "beneficiaryPersonType": "Tipo Pessoa Beneficiário",
                "beneficiaryDocumentId": "ID Documento Beneficiário",
                "beneficiaryName": "Nome Beneficiário",
                "attachedExpenseDocumentIndicator": "Indicador Anexo Documento Despesa",
                "expenseCategoryCode": "Código Categoria Despesa",
                "subtransactionAccountabilityIndicator": "Indicador Contabilidade Subtransação",
                "subtransactionAccountabilityName": "Nome Contabilidade Subtransação",
                "bankOrderPurposeCode": "Código Finalidade Ordem Banco",
                "bankOrderRuleCode": "Código Ordem Pagamento Banco",
                "bankOrderPurposeDescription": "Descrição Finalidade Ordem Banco",
                "expenseSequentialNumber": "Número Sequencial Despesa",
                "code": "Código Categoria Despesa",
                "parentCode": "Código Pai Categoria Despesa",
                "name": "Nome Categoria Despesa",
                "expensesDocuments": "Documentos Despesa",
            },
        )

    def get_extrato_fundos_investimento(
        self,
        agencia: int,
        conta_corrente: int,
        fundo_investimento_id: int,
        mes: int,
        ano: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/extratos/{agencia}-{conta_corrente}/fundos-investimentos/{fundo_investimento_id}",
            headers=common._get_headers(access_token),
            params={
                "mes": mes,
                "ano": ano,
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        res["extrato"]["valorCotaExtrato"] = res["extrato"].pop("valorCota")
        df = common._handle_results(
            res["extrato"],
            main_list="listaLancamentosExtrato",
            insertables=[
                "numeroAgenciaRecebedora",
                "digitoVerificadorContaRecebedora",
                "numeroContaCorrenteRecebedora",
                "numeroDigitoVerificadorContaCorrenteRecebedora",
                "nomeClienteRecebedor",
                "nomeFundoInvestimento",
                "CNPJFundoInvestimento",
                "valorCotaExtrato",
                "dataAfericaoValorCota",
                "ultimaCotacaoCota",
                "dataUltimaCotacaoCota",
                "sinalRentabilidadeMes",
                "valorRentabilidadeMes",
                "sinalRentabilidadeAno",
                "valorRentabilidadeAno",
                "sinalRentabilidadeResgateTotal",
                "valorRentabilidadeResgateTotal",
                "valorDisponivelResgate",
                "valorCarenciaResgate",
                "valorIRPrevisto",
                "percentualIRPrevisto",
                "valorIRComplementarPrevisto",
                "valorIOFPrevisto",
                "valorTaxaSaida",
                "valorBonusDesempenho",
                "valorBloqueado",
                "valorAplicado",
                "valorResgate",
                "valorSaldoAnterior",
                "quantidadeCotaAnterior",
                "dataSaldoAnterior",
                "valorTotalAplicadoPeriodo",
                "valorTotalResgatadoPeriodo",
                "sinalRendimentoBrutoPeriodo",
                "valorRendimentoBrutoPeriodo",
                "valorTotalIRPeriodo",
                "valorTotalIOFPeriodo",
                "valorTotalTaxaSaidaPeriodo",
                "valorTotalBonusDesempenhoPeriodo",
                "sinalRendimentoLiquido",
                "valorRendimentoLiquido",
                "valorSaldoMesAnterior",
                "quantidadeCotaMesAnterior",
                "dataSaldoMesAnterior",
                "numeroLancamento",
            ],
            rename_dict={
                "numeroAgenciaRecebedora": "Número Agência Recebedora",
                "digitoVerificadorContaRecebedora": "Dígito Verificador Conta Recebedora",
                "numeroContaCorrenteRecebedora": "Número Conta Corrente Recebedora",
                "numeroDigitoVerificadorContaCorrenteRecebedora": "Número Dígito Verificador Conta Corrente Recebedora",
                "nomeClienteRecebedor": "Nome Cliente Recebedor",
                "nomeFundoInvestimento": "Nome Fundo Investimento",
                "CNPJFundoInvestimento": " CNPJ Fundo Investimento",
                "valorCotaExtrato": "Valor Cota Extrato",
                "dataAfericaoValorCota": "Data Afericão Valor Cota",
                "ultimaCotacaoCota": "Última Cotação Cota",
                "dataUltimaCotacaoCota": "Data Última Cotação Cota",
                "sinalRentabilidadeMes": "Sinal Rentabilidade Mês",
                "valorRentabilidadeMes": "Valor Rentabilidade Mês",
                "sinalRentabilidadeAno": "Sinal Rentabilidade Ano",
                "valorRentabilidadeAno": "Valor Rentabilidade Ano",
                "sinalRentabilidadeResgateTotal": "Sinal Rentabilidade Resgate Total",
                "valorRentabilidadeResgateTotal": "Valor Rentabilidade Resgate Total",
                "valorDisponivelResgate": "Valor Disponível Resgate",
                "valorCarenciaResgate": "Valor Carência Resgate",
                "valorIRPrevisto": "Valor IR Previsto",
                "percentualIRPrevisto": "Percentual IR Previsto",
                "valorIRComplementarPrevisto": "Valor IR Complementar Previsto",
                "valorIOFPrevisto": "Valor IOF Previsto",
                "valorTaxaSaida": "Valor Taxa Saída",
                "valorBonusDesempenho": "Valor Bônus Desempenho",
                "valorBloqueado": "Valor Bloqueado",
                "valorAplicado": "Valor Aplicado",
                "valorResgate": "Valor Resgate",
                "valorSaldoAnterior": "Valor Saldo Anterior",
                "quantidadeCotaAnterior": "Quantidade Cota Anterior",
                "dataSaldoAnterior": "Data Saldo Anterior",
                "valorTotalAplicadoPeriodo": "Valor Total Aplicado Período",
                "valorTotalResgatadoPeriodo": "Valor Total Resgatado Período",
                "sinalRendimentoBrutoPeriodo": "Sinal Rendimento Bruto Período",
                "valorRendimentoBrutoPeriodo": "Valor Rendimento Bruto Período",
                "valorTotalIRPeriodo": "Valor Total IR Período",
                "valorTotalIOFPeriodo": "Valor Total IOF Período",
                "valorTotalTaxaSaidaPeriodo": "Valor Total Taxa Saída Período",
                "valorTotalBonusDesempenhoPeriodo": "Valor Total Bônus Desempenho Período",
                "sinalRendimentoLiquido": "Sinal Rendimento Líquido",
                "valorRendimentoLiquido": "Valor Rendimento Líquido",
                "valorSaldoMesAnterior": "Valor Saldo Mês Anterior",
                "quantidadeCotaMesAnterior": "Quantidade Cota Mês Anterior",
                "dataSaldoMesAnterior": "Data Saldo Mês Anterior",
                "numeroLancamento": "Número Lançamento",
                "dataLancamento": "Data Lançamento",
                "descricao": "Descrição",
                "valorLancamento": "Valor Lançamento",
                "valorIR": "Valor IR",
                "valorPrejuizo": "Valor Prejuízo",
                "valorIOF": "Valor IOF",
                "quantidadeCota": "Quantidade Cota",
                "valorCota": "Valor Cota Lançamento",
                "saldoCotas": "Saldo Cotas",
                "valorBaseCalculoIR": "Valor Base Cálculo IR",
                "numeroDocumentoLancamento": "Número Documento Lançamento",
            },
        )

        df["Código Programa Governo"] = res["codigoProgramaGoverno"]
        df["Nome Programa Governo"] = res["nomeProgramaGoverno"]
        df["Código SubPrograma Governo"] = res["codigoSubProgramaGoverno"]
        df["Nome SubPrograma Governo"] = res["nomeSubProgramaGoverno"]

        return df

    def get_extrato_poupanca(
        self,
        agencia: int,
        conta_corrente: int,
        variacao_poupanca: int,
        mes: int,
        ano: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/extratos/{agencia}-{conta_corrente}/poupanca/{variacao_poupanca}",
            headers=common._get_headers(access_token),
            params={
                "mes": mes,
                "ano": ano,
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaLancamentos",
            insertables=[
                "codigoProgramaGoverno",
                "nomeProgramaGoverno",
                "codigoSubProgramaGoverno",
                "nomeSubProgramaGoverno",
                "nomeCliente",
                "identificadorCliente",
                "saldoAnterior",
                "saldoAtual",
                "saldoBloqueado",
                "saldoDisponivel",
            ],
            rename_dict={
                "codigoProgramaGoverno": "Código Programa Governo",
                "nomeProgramaGoverno": "Nome Programa Governo",
                "codigoSubProgramaGoverno": "Código SubPrograma Governo",
                "nomeSubProgramaGoverno": "Nome SubPrograma Governo",
                "nomeCliente": "Nome Cliente",
                "identificadorCliente": "Identificador Cliente",
                "saldoAnterior": "Saldo Anterior",
                "saldoAtual": "Saldo Atual",
                "saldoBloqueado": "Saldo Bloqueado",
                "saldoDisponivel": "Saldo Disponível",
                "dataLancamento": "Data Lançamento",
                "dataMovimento": "Data Movimento",
                "diaLancamento": "Dia Lançamento",
                "codigoHistorico": "Código Histórico",
                "descricaoHistorico": "Descrição Histórico",
                "indicadorDebitoCredito": "Indicador Débito Crédito",
                "agenciaOrigem": "Agência Origem",
                "numeroDocumento": "Número Documento",
                "valorLancamento": "Valor Lançamento",
            },
        )

    def get_lancamentos_atualizados(
        self,
        numero_programa_governo: int,
        data_inicio: common.DateLike,
        data_fim: common.DateLike,
        pagina: int = 1,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        data_inicio = common._handle_dates(data_inicio)
        data_fim = common._handle_dates(data_fim)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/programas-governo/{numero_programa_governo}/orgaos-repasse/lancamentos-atualizados",
            headers=common._get_headers(access_token),
            params={
                "dataInicio": data_inicio,
                "dataFim": data_fim,
                "pagina": pagina,
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaLancamentos",
            insertables=[
                "totalPaginas",
            ],
            rename_dict={
                "totalPaginas": "Total Páginas",
                "agencia": "Agência",
                "contaCorrente": "Conta Corrente",
                "sequencialLancamento": "Sequencial Lançamento",
            },
        )

    def get_sublancamentos_atualizados(
        self,
        numero_programa_governo: int,
        data_inicio: common.DateLike,
        data_fim: common.DateLike,
        pagina: int = 1,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        data_inicio = common._handle_dates(data_inicio)
        data_fim = common._handle_dates(data_fim)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/programas-governo/{numero_programa_governo}/orgaos-repasse/sublancamentos-atualizados",
            headers=common._get_headers(access_token),
            params={
                "dataInicio": data_inicio,
                "dataFim": data_fim,
                "pagina": pagina,
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaSublancamentos",
            insertables=[
                "totalPaginas",
            ],
            rename_dict={
                "totalPaginas": "Total Páginas",
                "agencia": "Agência",
                "contaCorrente": "Conta Corrente",
                "sequencialLancamento": "Sequencial Lançamento",
                "sequencialSublancamento": "Sequencial Sublançamento",
            },
        )

    def get_categorias_programa_governo(
        self,
        numero_programa_governo: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/programas-governo/{numero_programa_governo}/categorias",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="categorias",
            rename_dict={
                "codigo": "Código Categoria",
                "nome": "Nome Categoria",
                "codigoCategoriaAgrupadora": "Código Categoria Agrupadora",
                "indicadorDespesaAtiva": "Indicador Despesa Ativa",
            },
        )

    def get_saldo_aplicacoes_financeiras(
        self,
        agencia: int,
        conta_corrente: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/saldos/{agencia}-{conta_corrente}/aplicacoes-financeiras",
            headers=common._get_headers(access_token),
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="operacoes",
            insertables=[
                "dataSaldo",
                "valorDisponibilidade",
            ],
            rename_dict={
                "dataSaldo": "Data Saldo",
                "valorDisponibilidade": "Valor Disponibilidade",
                "codigo": "Código",
                "valor": "Valor",
                "indicadorSaldoNaoDisponivel": "Indicador Saldo Não Disponível",
                "mensagemSaldoApurado": "Mensagem Saldo Apurado",
            },
        )

    def get_saldo_conta_corrente(
        self,
        agencia: str,
        conta_corrente: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/saldos/{agencia}-{conta_corrente}/conta-corrente",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            rename_dict={
                "dataSaldo": "Data Saldo",
                "valorDisponibilidade": "Valor Disponibilidade",
            },
        )

    def post_categoria_despesa_lancamento_credito(
        self,
        ordem_bancaria: int,
        item: int,
        agencia: int,
        conta_corrente: int,
        codigo_contrato: int,
        codigo_unidade_gestora: str,
        codigo_categoria_despesa: int,
        codigo_listagem_cliente: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "POST",
            f"{self._api_domain}/accountability/v3/orgaos-repasse/lancamentos-credito/{ordem_bancaria}-{item}/categorias-despesa",
            headers={
                "Content-Type": "application/json",
                **common._get_headers(access_token),
            },
            params={
                "gw-dev-app-key": self._app_key,
            },
            data={
                "agencia": agencia,
                "contaCorrente": conta_corrente,
                "codigoContrato": codigo_contrato,
                "codigoUnidadeGestora": codigo_unidade_gestora,
                "codigoCategoriaDespesa": codigo_categoria_despesa,
                "codigoListagemCliente": codigo_listagem_cliente,
            },
        )

        if res.status_code not in [200, 201]:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            rename_dict={
                "timestampInclusaoCategoriaDespesa": "Momento Inclusão Categoria Despesa",
            },
        )

    def post_identificacao_lancamento_credito(
        self,
        agencia: str,
        conta_corrente: str,
        numero_bancario: int,
        numero_sequencial_ordem_bancaria: int,
        data_lancamento: common.DateLike,
        numero_companhia: int,
        valor_fracionado: int,
        tipo_identificacao: int,
        codigo_identificacao: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        data_lancamento = common._handle_dates(data_lancamento)

        res = requests.request(
            "POST",
            f"{self._api_domain}/accountability/v3/orgaos-repasse/{agencia}-{conta_corrente}/lancamentos-credito",
            headers={
                "Content-Type": "application/json",
                **common._get_headers(access_token),
            },
            params={
                "gw-dev-app-key": self._app_key,
            },
            data={
                "numeroBancario": numero_bancario,
                "numeroSequencialOrdemBancaria": numero_sequencial_ordem_bancaria,
                "dataLancamento": data_lancamento,
                "numeroCompanhia": numero_companhia,
                "valorFracionado": valor_fracionado,
                "tipoIdentificacao": tipo_identificacao,
                "codigoIdentificacao": codigo_identificacao,
            },
        )

        if res.status_code not in [200, 201]:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            rename_dict={
                "numeroSequencialLancamentoContaCorrente": "Número Sequencial Lançamento Conta Corrente",
                "numeroSequencialIdentificacaoLancamento": "Número Sequencial Identificação Lançamento",
                "timestampInclusaoIdentificacaoLancamento": "Momento Inclusão Identificação Lançamento",
            },
        )

    def delete_identificacao_lancamento_credito(
        self,
        agencia: str,
        conta_corrente: str,
        sequencial_lancamento: str,
        sequencial_identificacao: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "DELETE",
            f"{self._api_domain}/accountability/v3/orgaos-repasse/{agencia}-{conta_corrente}/lancamentos-credito/{sequencial_lancamento}-{sequencial_identificacao}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code not in [200, 201]:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            rename_dict={
                "timestampExclusaoIdentificacaoLancamento": "Momento Exclusão Identificação Lançamento",
            },
        )

    def get_identificacao_lancamento_debito(
        self,
        agencia: str,
        conta_corrente: str,
        numero_pagina: int = 1,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/orgaos-repasse/{agencia}-{conta_corrente}/lancamentos-debito",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "numeroPagina": numero_pagina,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaLancamento",
            insertables=[
                "numeroPaginaTotal",
                "quantidadeIdentificacaoLancamento",
            ],
            rename_dict={
                "numeroPaginaTotal": "Número Página Total",
                "quantidadeIdentificacaoLancamento": "Quantidade Identificação Lançamento",
                "numeroSequencialLancamentoContaCorrente": "Número Sequencial Lançamento Conta Corrente",
                "numeroSequencialIdentificacaoLancamento": "Número Sequencial Identificação Lançamento",
                "tipoIdentificacao": "Tipo Identificação",
                "tipoIdentificacaoTexto": "Tipo Identificação Texto",
                "codigoIdentificacao": "Código Identificação",
                "numeroCompanhia": "Número Companhia",
                "valorFracionado": "Valor Fracionado",
            },
        )


class AccountabilityV3ControleAPI(_AccountabilityV3BaseAPI):
    """Representa um encapsulador da API Accountability V3 do Banco do Brasil
    para os órgaos de controle.

    Esse encapsulador já reutiliza o token de acesso por 10 minutos e gera um
    novo sempre que o atual estiver expirado.
    """

    def __init__(
        self,
        ambiente: common.Ambiente = common.Ambiente.HOMOLOGACAO,
        app_key: str = None,
        client_id: str = None,
        client_secret: str = None,
    ):
        super().__init__(
            ambiente,
            app_key,
            client_id,
            client_secret,
        )

    def get_extrato_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        start_date: common.DateLike,
        end_date: common.DateLike,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()
        start_date = common._handle_dates(start_date)
        end_date = common._handle_dates(end_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/statements/{branch_code}-{account_number}/control-agencies",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "startDate": start_date,
                "endDate": end_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        return common._handle_results(
            res,
            main_list="transactions",
            insertables=[
                "governmentProgramCode",
                "governmentProgramName",
                "governmentSubProgramCode",
                "governmentSubProgramName",
            ],
            explodeables=[
                "expensesDocuments",
            ],
            rename_dict={
                "governmentProgramCode": "Código Programa Governo",
                "governmentProgramName": "Nome Programa Governo",
                "governmentSubProgramCode": "Código SubPrograma Governo",
                "governmentSubProgramName": "Nome SubPrograma Governo",
                "id": "ID Transação",
                "bookingDate": "Data Agendamento",
                "orderIndex": "Índice Ordem",
                "valueDate": "Data Valor",
                "referenceNumber": "Número Referência",
                "value": "Valor",
                "accountBalance": "Saldo Conta",
                "descriptionCode": "Código Descrição",
                "descriptionName": "Nome Descrição",
                "descriptionBatchNumber": "Número Lote Descrição",
                "creditDebitIndicator": "Indicador Crédito Débito",
                "beneficiaryBankIdentifierCode": "Código Identificador Banco Beneficiário",
                "beneficiaryBranchCode": "Código Agência Beneficiário",
                "beneficiaryAccountNumber": "Número Conta Beneficiário",
                "beneficiaryPersonType": "Tipo Pessoa Beneficiário",
                "beneficiaryDocumentId": "ID Documento Beneficiário",
                "beneficiaryName": "Nome Beneficiário",
                "pendingExpenseConciliation": "Conciliação Despesa Pendente",
                "attachedExpenseDocumentIndicator": "Indicador Anexo Documento Despesa",
                "expenseCategoryCode": "Código Categoria Despesa",
                "expenseIdentificationStatus": "Status Identificação Despesa",
                "subTransactionQuantity": "Quantidade Subtransações",
                "bankOrderRuleCode": "Código Ordem Pagamento Banco",
                "bankOrderPurposeCode": "Código Finalidade Ordem Banco",
                "bankOrderPurposeDescription": "Descrição Finalidade Ordem Banco",
                "expenseSequentialNumber": "Número Sequencial Despesa",
                "expensesCategory": "Categoria Despesa",
                "expensesDocuments": "ID Documento Despesa",
            },
        )

    def get_documento_despesas_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        transaction_id: int,
        document_id: int,
        booking_date: common.DateLike,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        access_token = self._get_access_token()
        booking_date = common._handle_dates(booking_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/expenses/{branch_code}-{account_number}/transactions/{transaction_id}/documents/{document_id}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "bookingDate": booking_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        df_issuer = common._handle_results(
            res["issuer"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
            },
        )

        df_recipient = common._handle_results(
            res["recipient"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
                "presenceTypeCode": "Código Tipo Presença",
                "typeConsumerCode": "Código Tipo Consumidor",
            },
        )

        df_document = common._handle_results(
            res["expenseDocument"],
            main_list="items",
            insertables=[
                "accessKey",
                "receiptTypeCode",
                "typeCode",
                "serialCode",
                "number",
                "issueDate",
                "movementDate",
                "itemDeliveryDate",
                "value",
                "operationTypeName",
                "operation",
                "paymentMethod",
                "digitalSignatureCode",
                "pronafAbilityRegistration",
                "timestamp",
                "userId",
                "discountValue",
                "totalDiscountValue",
                "realeaseInstrumentCode",
                "realeaseInstrumentName",
                "realeaseInstrumentDate",
                "additionalInformation",
            ],
            rename_dict={
                "accessKey": "Chave Acesso",
                "receiptTypeCode": "Código Tipo Recibo",
                "typeCode": "Código Tipo",
                "serialCode": "Código Série",
                "number": "Número",
                "issueDate": "Data Emissão",
                "movementDate": "Data Movimentação",
                "itemDeliveryDate": "Data Entrega",
                "value": "Valor",
                "operationTypeName": "Nome Tipo Operação",
                "operation": "Operação",
                "paymentMethod": "Método Pagamento",
                "digitalSignatureCode": "Código Assinatura Digital",
                "pronafAbilityRegistration": "Registro Habilidade Pronaf",
                "timestamp": "Momento",
                "userId": "ID Usuário",
                "discountValue": "Valor Desconto",
                "totalDiscountValue": "Valor Total Desconto",
                "realeaseInstrumentCode": "Código Liberação de Instrumento",
                "realeaseInstrumentName": "Nome Liberação Instrumento",
                "realeaseInstrumentDate": "Data Liberação Instrumento",
                "additionalInformation": "Informação Adicional",
                "description": "Descrição Item",
                "quantity": "Quantidade Item",
                "metric": "Métrica Item",
                "unitValue": "Valor Unitário Item",
                "totalValue": "Valor Total Item",
                "mercosurCommonNameId": "ID Nome Comum Mercosul",
                "itemDiscountValue": "Valor Desconto Item",
            },
        )

        return (
            df_issuer,
            df_recipient,
            df_document,
        )

    def get_documento_despesas_prestacao_contas(
        self,
        branch_code: int,
        account_number: int,
        transaction_id: int,
        subtransaction_id: int,
        document_id: int,
        booking_date: common.DateLike,
    ) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        access_token = self._get_access_token()
        booking_date = common._handle_dates(booking_date)

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/expenses/{branch_code}-{account_number}/transactions/{transaction_id}/subTransactions/{subtransaction_id}/documents/{document_id}",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "bookingDate": booking_date,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível reaver o extrato do órgão repassador."
            )

        res = res.json()

        df_issuer = common._handle_results(
            res["issuer"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
            },
        )

        df_recipient = common._handle_results(
            res["recipient"],
            rename_dict={
                "corporateTaxPayerRegistry": "CNPJ",
                "individualTaxPayerRegistry": "CPF",
                "stateRegistrationNumber": "RG",
                "legalName": "Nome Legal",
                "tradeName": "Nome Social",
                "countryName": "Nacionalidade",
                "stateAbbreviation": "UF",
                "cityName": "Cidade",
                "districtName": "Bairro",
                "additionalAddressInformation": "Endereço",
                "postalCode": "CEP",
                "phoneNumber": "Telefone",
                "presenceTypeCode": "Código Tipo Presença",
                "typeConsumerCode": "Código Tipo Consumidor",
            },
        )

        df_document = common._handle_results(
            res["expenseDocument"],
            main_list="items",
            insertables=[
                "accessKey",
                "receiptTypeCode",
                "typeCode",
                "serialCode",
                "number",
                "issueDate",
                "movementDate",
                "itemDeliveryDate",
                "value",
                "operationTypeName",
                "operation",
                "paymentMethod",
                "digitalSignatureCode",
                "pronafAbilityRegistration",
                "timestamp",
                "userId",
                "discountValue",
                "totalDiscountValue",
                "realeaseInstrumentCode",
                "realeaseInstrumentName",
                "realeaseInstrumentDate",
                "additionalInformation",
            ],
            rename_dict={
                "accessKey": "Chave Acesso",
                "receiptTypeCode": "Código Tipo Recibo",
                "typeCode": "Código Tipo",
                "serialCode": "Código Série",
                "number": "Número",
                "issueDate": "Data Emissão",
                "movementDate": "Data Movimentação",
                "itemDeliveryDate": "Data Entrega",
                "value": "Valor",
                "operationTypeName": "Nome Tipo Operação",
                "operation": "Operação",
                "paymentMethod": "Método Pagamento",
                "digitalSignatureCode": "Código Assinatura Digital",
                "pronafAbilityRegistration": "Registro Habilidade Pronaf",
                "timestamp": "Momento",
                "userId": "ID Usuário",
                "discountValue": "Valor Desconto",
                "totalDiscountValue": "Valor Total Desconto",
                "realeaseInstrumentCode": "Código Liberação de Instrumento",
                "realeaseInstrumentName": "Nome Liberação Instrumento",
                "realeaseInstrumentDate": "Data Liberação Instrumento",
                "additionalInformation": "Informação Adicional",
                "description": "Descrição Item",
                "quantity": "Quantidade Item",
                "metric": "Métrica Item",
                "unitValue": "Valor Unitário Item",
                "totalValue": "Valor Total Item",
                "mercosurCommonNameId": "ID Nome Comum Mercosul",
                "itemDiscountValue": "Valor Desconto Item",
            },
        )

        return (
            df_issuer,
            df_recipient,
            df_document,
        )

    def get_extrato_subtransacoes_programa_governo(
        self,
        branch_code: int,
        account_number: int,
        id: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/statements/{branch_code}-{account_number}/debits/{id}/control-agencies/subtransactions",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="subtransactions",
            insertables=[
                "governmentProgramCode",
                "governmentProgramName",
                "governmentSubProgramCode",
                "governmentSubProgramName",
            ],
            explodeables=[
                "expensesCategory",
                "expensesDocuments",
            ],
            rename_dict={
                "governmentProgramCode": "Código Programa Governo",
                "governmentProgramName": "Nome Programa Governo",
                "governmentSubProgramCode": "Código SubPrograma Governo",
                "governmentSubProgramName": "Nome SubPrograma Governo",
                "id": "ID",
                "codeSubtransactionState": "Estado Código Subtransação",
                "paymentState": "Estado Pagamento",
                "paymentDate": "Data Pagamento",
                "value": "Valor",
                "beneficiaryBankIdentifierCode": "Código Identificador Banco Beneficiário",
                "beneficiaryBranchCode": "Código Agência Beneficiário",
                "beneficiaryAccountNumber": "Número Conta Beneficiário",
                "beneficiaryPersonType": "Tipo Pessoa Beneficiário",
                "beneficiaryDocumentId": "ID Documento Beneficiário",
                "beneficiaryName": "Nome Beneficiário",
                "attachedExpenseDocumentIndicator": "Indicador Anexo Documento Despesa",
                "expenseCategoryCode": "Código Categoria Despesa",
                "subtransactionAccountabilityIndicator": "Indicador Contabilidade Subtransação",
                "subtransactionAccountabilityName": "Nome Contabilidade Subtransação",
                "bankOrderPurposeCode": "Código Finalidade Ordem Banco",
                "bankOrderRuleCode": "Código Ordem Pagamento Banco",
                "bankOrderPurposeDescription": "Descrição Finalidade Ordem Banco",
                "expenseSequentialNumber": "Número Sequencial Despesa",
                "code": "Código Categoria Despesa",
                "parentCode": "Código Pai Categoria Despesa",
                "name": "Nome Categoria Despesa",
                "expensesDocuments": "Documentos Despesa",
            },
        )

    def get_extrato_fundos_investimento(
        self,
        agencia: str,
        conta_corrente: str,
        fundo_investimento_id: str,
        mes: int,
        ano: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/extratos/{agencia}-{conta_corrente}/fundos-investimentos/{fundo_investimento_id}/control-agencies",
            headers=common._get_headers(access_token),
            params={
                "mes": mes,
                "ano": ano,
                "gw-dev-app-key": self._app_key,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        res["extrato"]["valorCotaExtrato"] = res["extrato"].pop("valorCota")
        df = common._handle_results(
            res["extrato"],
            main_list="listaLancamentosExtrato",
            insertables=[
                "numeroAgenciaRecebedora",
                "digitoVerificadorContaRecebedora",
                "numeroContaCorrenteRecebedora",
                "numeroDigitoVerificadorContaCorrenteRecebedora",
                "nomeClienteRecebedor",
                "nomeFundoInvestimento",
                "CNPJFundoInvestimento",
                "valorCotaExtrato",
                "dataAfericaoValorCota",
                "ultimaCotacaoCota",
                "dataUltimaCotacaoCota",
                "sinalRentabilidadeMes",
                "valorRentabilidadeMes",
                "sinalRentabilidadeAno",
                "valorRentabilidadeAno",
                "sinalRentabilidadeResgateTotal",
                "valorRentabilidadeResgateTotal",
                "valorDisponivelResgate",
                "valorCarenciaResgate",
                "valorIRPrevisto",
                "percentualIRPrevisto",
                "valorIRComplementarPrevisto",
                "valorIOFPrevisto",
                "valorTaxaSaida",
                "valorBonusDesempenho",
                "valorBloqueado",
                "valorAplicado",
                "valorResgate",
                "valorSaldoAnterior",
                "quantidadeCotaAnterior",
                "dataSaldoAnterior",
                "valorTotalAplicadoPeriodo",
                "valorTotalResgatadoPeriodo",
                "sinalRendimentoBrutoPeriodo",
                "valorRendimentoBrutoPeriodo",
                "valorTotalIRPeriodo",
                "valorTotalIOFPeriodo",
                "valorTotalTaxaSaidaPeriodo",
                "valorTotalBonusDesempenhoPeriodo",
                "sinalRendimentoLiquido",
                "valorRendimentoLiquido",
                "valorSaldoMesAnterior",
                "quantidadeCotaMesAnterior",
                "dataSaldoMesAnterior",
                "numeroLancamento",
            ],
            rename_dict={
                "numeroAgenciaRecebedora": "Número Agência Recebedora",
                "digitoVerificadorContaRecebedora": "Dígito Verificador Conta Recebedora",
                "numeroContaCorrenteRecebedora": "Número Conta Corrente Recebedora",
                "numeroDigitoVerificadorContaCorrenteRecebedora": "Número Dígito Verificador Conta Corrente Recebedora",
                "nomeClienteRecebedor": "Nome Cliente Recebedor",
                "nomeFundoInvestimento": "Nome Fundo Investimento",
                "CNPJFundoInvestimento": " CNPJ Fundo Investimento",
                "valorCotaExtrato": "Valor Cota Extrato",
                "dataAfericaoValorCota": "Data Afericão Valor Cota",
                "ultimaCotacaoCota": "Última Cotação Cota",
                "dataUltimaCotacaoCota": "Data Última Cotação Cota",
                "sinalRentabilidadeMes": "Sinal Rentabilidade Mês",
                "valorRentabilidadeMes": "Valor Rentabilidade Mês",
                "sinalRentabilidadeAno": "Sinal Rentabilidade Ano",
                "valorRentabilidadeAno": "Valor Rentabilidade Ano",
                "sinalRentabilidadeResgateTotal": "Sinal Rentabilidade Resgate Total",
                "valorRentabilidadeResgateTotal": "Valor Rentabilidade Resgate Total",
                "valorDisponivelResgate": "Valor Disponível Resgate",
                "valorCarenciaResgate": "Valor Carência Resgate",
                "valorIRPrevisto": "Valor IR Previsto",
                "percentualIRPrevisto": "Percentual IR Previsto",
                "valorIRComplementarPrevisto": "Valor IR Complementar Previsto",
                "valorIOFPrevisto": "Valor IOF Previsto",
                "valorTaxaSaida": "Valor Taxa Saída",
                "valorBonusDesempenho": "Valor Bônus Desempenho",
                "valorBloqueado": "Valor Bloqueado",
                "valorAplicado": "Valor Aplicado",
                "valorResgate": "Valor Resgate",
                "valorSaldoAnterior": "Valor Saldo Anterior",
                "quantidadeCotaAnterior": "Quantidade Cota Anterior",
                "dataSaldoAnterior": "Data Saldo Anterior",
                "valorTotalAplicadoPeriodo": "Valor Total Aplicado Período",
                "valorTotalResgatadoPeriodo": "Valor Total Resgatado Período",
                "sinalRendimentoBrutoPeriodo": "Sinal Rendimento Bruto Período",
                "valorRendimentoBrutoPeriodo": "Valor Rendimento Bruto Período",
                "valorTotalIRPeriodo": "Valor Total IR Período",
                "valorTotalIOFPeriodo": "Valor Total IOF Período",
                "valorTotalTaxaSaidaPeriodo": "Valor Total Taxa Saída Período",
                "valorTotalBonusDesempenhoPeriodo": "Valor Total Bônus Desempenho Período",
                "sinalRendimentoLiquido": "Sinal Rendimento Líquido",
                "valorRendimentoLiquido": "Valor Rendimento Líquido",
                "valorSaldoMesAnterior": "Valor Saldo Mês Anterior",
                "quantidadeCotaMesAnterior": "Quantidade Cota Mês Anterior",
                "dataSaldoMesAnterior": "Data Saldo Mês Anterior",
                "numeroLancamento": "Número Lançamento",
                "dataLancamento": "Data Lançamento",
                "descricao": "Descrição",
                "valorLancamento": "Valor Lançamento",
                "valorIR": "Valor IR",
                "valorPrejuizo": "Valor Prejuízo",
                "valorIOF": "Valor IOF",
                "quantidadeCota": "Quantidade Cota",
                "valorCota": "Valor Cota Lançamento",
                "saldoCotas": "Saldo Cotas",
                "valorBaseCalculoIR": "Valor Base Cálculo IR",
                "numeroDocumentoLancamento": "Número Documento Lançamento",
            },
        )

        df["Código Programa Governo"] = res["codigoProgramaGoverno"]
        df["Nome Programa Governo"] = res["nomeProgramaGoverno"]
        df["Código SubPrograma Governo"] = res["codigoSubProgramaGoverno"]
        df["Nome SubPrograma Governo"] = res["nomeSubProgramaGoverno"]

        return df

    def get_extrato_poupanca(
        self,
        agencia: str,
        conta_corrente: str,
        variacao_poupanca: str,
        codigo_variacao: int,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/extratos/{agencia}-{conta_corrente}/poupanca/{variacao_poupanca}/orgao-controle",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "codigoVariacao": codigo_variacao,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaLancamentos",
            insertables=[
                "codigoProgramaGoverno",
                "nomeProgramaGoverno",
                "codigoSubProgramaGoverno",
                "nomeSubProgramaGoverno",
                "nomeCliente",
                "identificadorCliente",
                "saldoAnterior",
                "saldoAtual",
                "saldoBloqueado",
                "saldoDisponivel",
            ],
            rename_dict={
                "codigoProgramaGoverno": "Código Programa Governo",
                "nomeProgramaGoverno": "Nome Programa Governo",
                "codigoSubProgramaGoverno": "Código SubPrograma Governo",
                "nomeSubProgramaGoverno": "Nome SubPrograma Governo",
                "nomeCliente": "Nome Cliente",
                "identificadorCliente": "Identificador Cliente",
                "saldoAnterior": "Saldo Anterior",
                "saldoAtual": "Saldo Atual",
                "saldoBloqueado": "Saldo Bloqueado",
                "saldoDisponivel": "Saldo Disponível",
                "dataLancamento": "Data Lançamento",
                "dataMovimento": "Data Movimento",
                "diaLancamento": "Dia Lançamento",
                "codigoHistorico": "Código Histórico",
                "descricaoHistorico": "Descrição Histórico",
                "indicadorDebitoCredito": "Indicador Débito Crédito",
                "agenciaOrigem": "Agência Origem",
                "numeroDocumento": "Número Documento",
                "valorLancamento": "Valor Lançamento",
            },
        )

    def get_contas_correntes(
        self,
        numero_registro: str,
    ) -> pd.DataFrame:
        access_token = self._get_access_token()

        res = requests.request(
            "GET",
            f"{self._api_domain}/accountability/v3/conta-corrente/orgaos-controle",
            headers=common._get_headers(access_token),
            params={
                "gw-dev-app-key": self._app_key,
                "numeroRegistro": numero_registro,
            },
        )

        if res.status_code != 200:
            raise Exception(
                "Não foi possível listar as categorias do programa de governo."
            )

        res = res.json()
        return common._handle_results(
            res,
            main_list="listaContaCorrente",
            insertables=[
                "numeroRegistroConsultar",
                "quantidadeContaCorrente",
            ],
            rename_dict={
                "numeroRegistroConsultar": "Número Registro Consultar",
                "quantidadeContaCorrente": "Quantidade Conta Corrente",
                "codigoProgramaGoverno": "Código Programa Governo",
                "nomeProgramaGoverno": "Nome Programa Governo",
                "cnpj": "CNPJ",
                "agencia": "Agência",
                "nomeAgencia": "Nome Agência",
                "contaCorrente": "Conta Corrente",
            },
        )
