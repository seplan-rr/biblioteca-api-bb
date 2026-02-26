import re
import pandas as pd
from enum import Enum
from typing import Any, Dict, NewType, Sequence, Union
from datetime import date, datetime, timedelta

_dese_oauth_domain = "https://oauth.desenv.bb.com.br"
_homo_oauth_domain = "https://oauth.hm.bb.com.br"
_homo_alt_oauth_domain = "https://oauth.sandbox.bb.com.br"
_prod_oauth_domain = "https://oauth.bb.com.br"

_dese_api_domain = "https://api.desenv.bb.com.br"
_homo_api_domain = "https://api.hm.bb.com.br"
_homo_alt_api_domain = "https://api.sandbox.bb.com.br"
_prod_api_domain = "https://api.bb.com.br"

_time_between_access_token_requests = timedelta(minutes=10)


DateLike = NewType("DateLike", Union[str | date | datetime])


class Ambiente(Enum):
    DESENVOLVIMENTO = 0
    HOMOLOGACAO = 1
    HOMOLOGACAO_ALTERNATIVO = 2
    PRODUCAO = 3


def _get_headers(access_token: str) -> Dict:
    return {
        "Authorization": f"Bearer {access_token}",
    }


def _handle_numeric_string_with_symbols(v: str) -> str:
    return re.sub(r"\D", "", v)


def _handle_dates(v: DateLike) -> str:
    if isinstance(v, str):
        v = datetime.strptime(v, "%Y-%m-%d")
    elif isinstance(v, date):
        v = datetime.combine(v, datetime.min.time())

    v = v.strftime("%Y-%m-%d")


def _handle_results(
    data: Any,
    main_list: str = None,
    insertables: Sequence[str] = None,
    explodeables: Sequence[str] = None,
    rename_dict: Dict[str, str] = None,
) -> pd.DataFrame:
    if main_list is not None:
        df = pd.DataFrame(data[main_list])
    else:
        df = pd.DataFrame(data)

    if insertables is not None:
        for insertable in insertables:
            df[insertable] = data[insertable]

    if explodeables is not None:
        for explodeable in explodeables:
            df = df.explode(explodeable, ignore_index=True)

    if rename_dict is not None:
        df = df.rename(rename_dict, axis=1)

    return df
