import io
import os
import signal
import time
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from faker import Faker


def generate_iot_sensor_data(num_rows: int = 1000) -> pd.DataFrame:
    """Genera datos sinteticos de sensores IoT con timestamp en microsegundos."""
    fake = Faker()
    now = datetime.utcnow()

    data = []
    for i in range(num_rows):
        ts_dt = now - timedelta(seconds=i * fake.random_int(min=1, max=60))
        timestamp_us = int(ts_dt.timestamp() * 1_000_000)
        data.append(
            {
                "id_sensor": f"sensor_{fake.random_int(min=1000, max=9999)}",
                "temperatura": round(
                    fake.pyfloat(
                        left_digits=2,
                        right_digits=2,
                        positive=True,
                        min_value=10,
                        max_value=45,
                    ),
                    2,
                ),
                "humedad": round(
                    fake.pyfloat(
                        left_digits=2,
                        right_digits=2,
                        positive=True,
                        min_value=20,
                        max_value=95,
                    ),
                    2,
                ),
                "estado": fake.random_element(
                    elements=("activo", "inactivo", "mantenimiento", "error")
                ),
                "timestamp": timestamp_us,
            }
        )

    df = pd.DataFrame(data)
    df = df[["id_sensor", "temperatura", "humedad", "estado", "timestamp"]]
    return df.sort_values("timestamp").reset_index(drop=True)


def save_parquet(df: pd.DataFrame, output_dir: Path) -> Path:
    """Guarda el dataset en Parquet con nombre unico basado en timestamp UTC."""
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"sensor_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.parquet"
    file_path = output_dir / filename
    expected_cols = ["id_sensor", "temperatura", "humedad", "estado", "timestamp"]
    df = df[expected_cols]
    df.to_parquet(file_path, index=False)
    return file_path


def upload_to_databricks(local_file_path: Path, volume_path: str) -> None:
    """Sube el archivo a Unity Catalog Volume usando databricks-sdk."""
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")

    if not host or not token:
        raise EnvironmentError(
            "Las variables DATABRICKS_HOST y DATABRICKS_TOKEN son obligatorias"
        )

    w = WorkspaceClient(host=host, token=token)

    with local_file_path.open("rb") as f:
        file_bytes = f.read()

    remote_file_path = f"{volume_path.rstrip('/')}/{local_file_path.name}"

    w.files.upload(
        file_path=remote_file_path,
        contents=io.BytesIO(file_bytes),
        overwrite=True,
    )

    file_size_bytes = local_file_path.stat().st_size
    print(f"Archivo subido exitosamente a: {remote_file_path}")
    print(f"Tamano del archivo subido: {file_size_bytes} bytes")


def main() -> None:
    num_rows = int(os.getenv("NUM_ROWS", "1000"))
    local_output_dir = Path(os.getenv("LOCAL_OUTPUT_DIR", "./output"))
    volume_path = os.getenv(
        "DATABRICKS_VOLUME_PATH", "/Volumes/workspace/default/iot_device/"
    )
    interval_seconds = int(os.getenv("INTERVAL_SECONDS", "30"))
    running = True

    def stop_handler(signum, frame):
        nonlocal running
        running = False
        print(f"Senal {signum} recibida. Finalizando proceso...")

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    print(
        "Iniciando generacion continua: "
        f"{num_rows} registros por archivo, cada {interval_seconds} segundos."
    )

    while running:
        print(f"Generando {num_rows} registros sinteticos de sensores IoT...")
        df = generate_iot_sensor_data(num_rows=num_rows)

        local_parquet_path = save_parquet(df, local_output_dir)
        print(f"Archivo Parquet generado localmente en: {local_parquet_path}")

        upload_to_databricks(local_parquet_path, volume_path)

        if running:
            print(
                f"Esperando {interval_seconds} segundos para la siguiente iteracion..."
            )
            time.sleep(interval_seconds)

    print("Proceso finalizado correctamente.")


if __name__ == "__main__":
    main()
