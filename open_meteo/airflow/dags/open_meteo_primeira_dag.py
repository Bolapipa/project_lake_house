from datetime import datetime, timedelta, timezone
import json
import os
from pathlib import Path
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator


CAPITAIS_BRASIL = [
    {"uf": "AC", "capital": "Rio Branco", "latitude": -9.97499, "longitude": -67.8243},
    {"uf": "AL", "capital": "Maceio", "latitude": -9.66599, "longitude": -35.7350},
    {"uf": "AP", "capital": "Macapa", "latitude": 0.03493, "longitude": -51.0694},
    {"uf": "AM", "capital": "Manaus", "latitude": -3.11903, "longitude": -60.0217},
    {"uf": "BA", "capital": "Salvador", "latitude": -12.9714, "longitude": -38.5014},
    {"uf": "CE", "capital": "Fortaleza", "latitude": -3.73186, "longitude": -38.5267},
    {"uf": "DF", "capital": "Brasilia", "latitude": -15.7939, "longitude": -47.8828},
    {"uf": "ES", "capital": "Vitoria", "latitude": -20.3155, "longitude": -40.3128},
    {"uf": "GO", "capital": "Goiania", "latitude": -16.6869, "longitude": -49.2648},
    {"uf": "MA", "capital": "Sao Luis", "latitude": -2.53073, "longitude": -44.3068},
    {"uf": "MT", "capital": "Cuiaba", "latitude": -15.6010, "longitude": -56.0974},
    {"uf": "MS", "capital": "Campo Grande", "latitude": -20.4697, "longitude": -54.6201},
    {"uf": "MG", "capital": "Belo Horizonte", "latitude": -19.9167, "longitude": -43.9345},
    {"uf": "PA", "capital": "Belem", "latitude": -1.45583, "longitude": -48.5044},
    {"uf": "PB", "capital": "Joao Pessoa", "latitude": -7.1195, "longitude": -34.8450},
    {"uf": "PR", "capital": "Curitiba", "latitude": -25.4284, "longitude": -49.2733},
    {"uf": "PE", "capital": "Recife", "latitude": -8.04756, "longitude": -34.8770},
    {"uf": "PI", "capital": "Teresina", "latitude": -5.0892, "longitude": -42.8016},
    {"uf": "RJ", "capital": "Rio de Janeiro", "latitude": -22.9068, "longitude": -43.1729},
    {"uf": "RN", "capital": "Natal", "latitude": -5.79448, "longitude": -35.2110},
    {"uf": "RS", "capital": "Porto Alegre", "latitude": -30.0346, "longitude": -51.2177},
    {"uf": "RO", "capital": "Porto Velho", "latitude": -8.76077, "longitude": -63.8999},
    {"uf": "RR", "capital": "Boa Vista", "latitude": 2.82384, "longitude": -60.6753},
    {"uf": "SC", "capital": "Florianopolis", "latitude": -27.5949, "longitude": -48.5482},
    {"uf": "SP", "capital": "Sao Paulo", "latitude": -23.5505, "longitude": -46.6333},
    {"uf": "SE", "capital": "Aracaju", "latitude": -10.9472, "longitude": -37.0731},
    {"uf": "TO", "capital": "Palmas", "latitude": -10.1840, "longitude": -48.3336},
]

LOCAL_TIMEZONE = "America/Sao_Paulo"
DEFAULT_FORECAST_DAYS = 7
DEFAULT_HTTP_TIMEOUT_SECONDS = 60
DEFAULT_HTTP_MAX_RETRIES = 3
DEFAULT_HTTP_RETRY_WAIT_SECONDS = 5
DEFAULT_OUTPUT_DIR = "/opt/airflow/config/open_meteo_bronze"


def _read_int_env(key: str, default: int) -> int:
    raw_value = os.getenv(key)
    if raw_value is None or raw_value.strip() == "":
        return default
    return int(raw_value)


FORECAST_DAYS = _read_int_env("OPEN_METEO_FORECAST_DAYS", DEFAULT_FORECAST_DAYS)
HTTP_TIMEOUT_SECONDS = _read_int_env("OPEN_METEO_API_TIMEOUT_SECONDS", DEFAULT_HTTP_TIMEOUT_SECONDS)
HTTP_MAX_RETRIES = _read_int_env("OPEN_METEO_API_MAX_RETRIES", DEFAULT_HTTP_MAX_RETRIES)
HTTP_RETRY_WAIT_SECONDS = _read_int_env(
    "OPEN_METEO_API_RETRY_WAIT_SECONDS",
    DEFAULT_HTTP_RETRY_WAIT_SECONDS,
)
BRONZE_OUTPUT_DIR = Path(os.getenv("OPEN_METEO_BRONZE_OUTPUT_DIR", DEFAULT_OUTPUT_DIR))


def montar_url(latitude, longitude):
    parametros = {
        "latitude": latitude,
        "longitude": longitude,
        "daily": (
            "temperature_2m_max,"
            "temperature_2m_min,"
            "temperature_2m_mean,"
            "precipitation_sum,"
            "weather_code,"
            "wind_speed_10m_max,"
            "sunrise,"
            "sunset"
        ),
        "timezone": LOCAL_TIMEZONE,
        "forecast_days": FORECAST_DAYS,
    }

    return f"https://api.open-meteo.com/v1/forecast?{urlencode(parametros)}"


def consultar_open_meteo(url: str) -> dict:
    tentativas = HTTP_MAX_RETRIES + 1

    for tentativa in range(1, tentativas + 1):
        try:
            with urlopen(url, timeout=HTTP_TIMEOUT_SECONDS) as response:
                return json.loads(response.read().decode("utf-8"))
        except (HTTPError, URLError, TimeoutError) as error:
            if tentativa == tentativas:
                raise RuntimeError(
                    f"Falha ao consultar Open-Meteo apos {tentativas} tentativas. URL: {url}"
                ) from error

            tempo_espera = HTTP_RETRY_WAIT_SECONDS * tentativa
            print(
                f"Tentativa {tentativa}/{tentativas} falhou ({error}). "
                f"Nova tentativa em {tempo_espera}s."
            )
            time.sleep(tempo_espera)


def extrair_clima_brasil():
    data_coleta = datetime.now(timezone.utc)
    registros_bronze = []

    for localidade in CAPITAIS_BRASIL:
        url = montar_url(localidade["latitude"], localidade["longitude"])
        resposta_api = consultar_open_meteo(url)

        registro = {
            "data_coleta_utc": data_coleta.isoformat(),
            "uf": localidade["uf"],
            "capital": localidade["capital"],
            "latitude": localidade["latitude"],
            "longitude": localidade["longitude"],
            "url": url,
            "resposta_api": resposta_api,
        }

        registros_bronze.append(registro)
        print(
            f"Coleta concluida: {localidade['uf']} - {localidade['capital']} "
            f"(previsao de {FORECAST_DAYS} dias)"
        )

    BRONZE_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    nome_arquivo = f"open_meteo_bronze_{data_coleta.strftime('%Y%m%dT%H%M%SZ')}.json"
    arquivo_destino = BRONZE_OUTPUT_DIR / nome_arquivo

    with arquivo_destino.open("w", encoding="utf-8") as arquivo:
        json.dump(registros_bronze, arquivo, ensure_ascii=False, indent=2)

    print(f"Arquivo bronze gerado em: {arquivo_destino}")
    print(f"Total de registros salvos: {len(registros_bronze)}")

    return str(arquivo_destino)


with DAG(
    dag_id="open_meteo_primeira_dag",
    start_date=pendulum.datetime(2024, 1, 1, tz=LOCAL_TIMEZONE),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data_engineering",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["open_meteo", "api", "bronze"],
) as dag:
    extrair_dados = PythonOperator(
        task_id="extrair_clima_brasil",
        python_callable=extrair_clima_brasil,
    )

    extrair_dados
