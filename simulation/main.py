import io
import os
import signal
import time
from datetime import datetime
from pathlib import Path

import pandas as pd
from databricks.sdk import WorkspaceClient
from faker import Faker

SENSOR_MASTER_VOLUME_PATH = "/Volumes/workspace/projects/iot_devices/iot_metadata/"
TELEMETRY_VOLUME_PATH = "/Volumes/workspace/projects/iot_devices/iot_telemetry/"
DEFAULT_INTERVAL_SECONDS = 40
SENSOR_MASTER_COUNT = 25

SENSOR_PROFILES = [
    ("MX-100", "Planta_A", 45.0),
    ("MX-100", "Planta_B", 45.0),
    ("MX-200", "Planta_C", 50.0),
    ("MX-200", "Planta_D", 50.0),
    ("AX-310", "Planta_E", 55.0),
    ("AX-310", "Planta_F", 55.0),
    ("TX-900", "Planta_G", 60.0),
    ("TX-900", "Planta_H", 60.0),
    ("QX-77", "Planta_I", 48.0),
    ("QX-77", "Planta_J", 48.0),
]


def get_workspace_client() -> WorkspaceClient:
    host = os.getenv("DATABRICKS_HOST")
    token = os.getenv("DATABRICKS_TOKEN")
    if not host or not token:
        raise EnvironmentError(
            "Las variables DATABRICKS_HOST y DATABRICKS_TOKEN son obligatorias."
        )
    return WorkspaceClient(host=host, token=token)


def save_parquet(df: pd.DataFrame, output_dir: Path, prefix: str) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{prefix}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S_%f')}.parquet"
    file_path = output_dir / filename
    df.to_parquet(file_path, index=False)
    return file_path


def upload_to_volume(w: WorkspaceClient, local_file_path: Path, volume_path: str) -> None:
    remote_file_path = f"{volume_path.rstrip('/')}/{local_file_path.name}"
    with local_file_path.open("rb") as f:
        w.files.upload(
            file_path=remote_file_path,
            contents=io.BytesIO(f.read()),
            overwrite=True,
        )

    file_size_bytes = local_file_path.stat().st_size
    print(f"Archivo subido exitosamente a: {remote_file_path}")
    print(f"Tamano del archivo subido: {file_size_bytes} bytes")


def create_sensor_master() -> pd.DataFrame:
    rows = []
    for i in range(SENSOR_MASTER_COUNT):
        modelo, ubicacion, rango_max = SENSOR_PROFILES[i % len(SENSOR_PROFILES)]
        rows.append(
            {
                "id_sensor": f"sensor_{i + 1:03d}",
                "modelo": modelo,
                "ubicacion": ubicacion,
                "rango_max": rango_max,
            }
        )
    return pd.DataFrame(rows)


def create_telemetry_snapshot(sensor_ids: list[str], fake: Faker) -> pd.DataFrame:
    timestamp_us = int(datetime.utcnow().timestamp() * 1_000_000)
    rows = []
    for sensor_id in sensor_ids:
        rows.append(
            {
                "id_sensor": sensor_id,
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
                "timestamp_us": timestamp_us,
            }
        )
    return pd.DataFrame(rows)


def run_once_master(w: WorkspaceClient, output_dir: Path, volume_path: str) -> list[str]:
    print("Generando archivo maestro de sensores (ejecucion unica)...")
    df_master = create_sensor_master()
    local_file = save_parquet(df_master, output_dir, "sensor_master")
    print(f"Archivo maestro generado localmente en: {local_file}")
    upload_to_volume(w, local_file, volume_path)
    return df_master["id_sensor"].tolist()


def run_telemetry_loop(
    w: WorkspaceClient,
    sensor_ids: list[str],
    output_dir: Path,
    volume_path: str,
    interval_seconds: int,
) -> None:
    fake = Faker()
    running = True

    def stop_handler(signum, frame):
        nonlocal running
        running = False
        print(f"Senal {signum} recibida. Finalizando loop de telemetria...")

    signal.signal(signal.SIGINT, stop_handler)
    signal.signal(signal.SIGTERM, stop_handler)

    print(
        f"Iniciando loop de telemetria para {len(sensor_ids)} sensores cada {interval_seconds} segundos."
    )

    while running:
        df_telemetry = create_telemetry_snapshot(sensor_ids, fake)
        local_file = save_parquet(df_telemetry, output_dir, "iot_telemetry")
        print(f"Archivo de telemetria generado localmente en: {local_file}")
        upload_to_volume(w, local_file, volume_path)

        if running:
            print(f"Esperando {interval_seconds} segundos para la siguiente iteracion...")
            time.sleep(interval_seconds)

    print("Loop de telemetria finalizado.")


def main() -> None:
    w = get_workspace_client()

    master_output_dir = Path(os.getenv("MASTER_LOCAL_OUTPUT_DIR", "./output/master"))
    telemetry_output_dir = Path(
        os.getenv("TELEMETRY_LOCAL_OUTPUT_DIR", "./output/telemetry")
    )
    master_volume_path = os.getenv(
        "MASTER_VOLUME_PATH",
        SENSOR_MASTER_VOLUME_PATH,
    )
    telemetry_volume_path = os.getenv(
        "TELEMETRY_VOLUME_PATH",
        TELEMETRY_VOLUME_PATH,
    )
    interval_seconds = int(
        os.getenv("TELEMETRY_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS))
    )

    sensor_ids = run_once_master(w, master_output_dir, master_volume_path)
    run_telemetry_loop(
        w=w,
        sensor_ids=sensor_ids,
        output_dir=telemetry_output_dir,
        volume_path=telemetry_volume_path,
        interval_seconds=interval_seconds,
    )


if __name__ == "__main__":
    main()
