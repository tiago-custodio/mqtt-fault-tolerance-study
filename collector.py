#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# collector.py
# Coleta taxa de entrega do receiver e sender + uso de CPU de containers Docker,
# salvando tudo em um CSV com amostras em intervalo fixo.

import argparse
import csv
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional
from docker.models.containers import Container

import requests

# Dependências:
#   pip install requests docker
try:
    import docker
    from docker.errors import NotFound, DockerException
except Exception:  # se docker não estiver instalado
    docker = None
    NotFound = DockerException = Exception


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_metrics(base_url: str, window: Optional[int] = None, timeout: float = 3.0) -> Dict:
    """
    Lê /metrics de um serviço (receiver ou sender).
    Se 'window' for fornecido, envia como parâmetro.
    """
    if window is not None:
        url = f"{base_url.rstrip('/')}/metrics?window={int(window)}"
    else:
        url = f"{base_url.rstrip('/')}/metrics"
    r = requests.get(url, timeout=timeout)
    r.raise_for_status()
    return r.json()


def cpu_percent_from_stats(stats: Dict) -> Optional[float]:
    """
    Calcula %CPU a partir de docker stats (stream=False).
    Fórmula padrão Docker (cgroups v1/v2).
    """
    try:
        cpu_stats = stats["cpu_stats"]
        precpu_stats = stats.get("precpu_stats", {})
        if not precpu_stats or "system_cpu_usage" not in precpu_stats:
            return None

        cpu_delta = (
            cpu_stats["cpu_usage"]["total_usage"]
            - precpu_stats["cpu_usage"]["total_usage"]
        )
        system_delta = (
            cpu_stats["system_cpu_usage"]
            - precpu_stats["system_cpu_usage"]
        )

        online_cpus = cpu_stats.get("online_cpus")
        if not online_cpus:
            online_cpus = len(cpu_stats["cpu_usage"].get("percpu_usage", [])) or 1

        if system_delta > 0 and cpu_delta >= 0:
            return (cpu_delta / system_delta) * online_cpus * 100.0
        return None
    except Exception:
        return None


class DockerCPUReader:
    def __init__(self, container_names: List[str], verbose: bool = False):
        self.verbose = verbose
        self.client = None
        self._names = container_names
        self._resolved: Dict[str, Optional[List[Container]]] = {}

        if docker is None:
            if self.verbose:
                print("[DEBUG] Módulo 'docker' não está disponível. CPU ficará None.")
            return

        try:
            self.client = docker.from_env()
            _ = self.client.ping()
            if self.verbose:
                print("[DEBUG] Conexão com Docker OK.")
        except Exception as e:
            self.client = None
            print(f"[WARN] Não foi possível acessar o Docker: {e}")
            print("       Sugestão: adicione seu usuário ao grupo 'docker' e faça logout/login,")
            print("       ou execute o collector com 'sudo'. CPU ficará None.")

    def _resolve_list(self, name: str) -> Optional[List[Container]]:
        if name in self._resolved:
            return self._resolved[name]

        if self.client is None:
            self._resolved[name] = None
            return None

        try:
            c = self.client.containers.get(name)
            self._resolved[name] = [c]
            return self._resolved[name]
        except NotFound:
            pass
        except Exception as e:
            if self.verbose:
                print(f"[DEBUG] Erro nome exato: {e}")

        try:
            found = self.client.containers.list(
                all=False,
                filters={"label": f"com.docker.compose.service={name}"}
            )
            if found:
                self._resolved[name] = found
                return found
        except Exception as e:
            if self.verbose:
                print(f"[DEBUG] Erro label compose: {e}")

        try:
            all_running = self.client.containers.list(all=False)
            matches = [c for c in all_running if c.name.endswith(f"-{name}") or f"-{name}-" in c.name]
            if matches:
                self._resolved[name] = matches
                return matches
        except Exception as e:
            if self.verbose:
                print(f"[DEBUG] Erro padrão de nome: {e}")

        self._resolved[name] = None
        return None

    def read_all(self) -> Dict[str, Optional[float]]:
        out: Dict[str, Optional[float]] = {}
        for name in self._names:
            containers = self._resolve_list(name)
            if not containers:
                out[name] = None
                continue

            agg = 0.0
            have_any = False
            for cont in containers:
                try:
                    stats = cont.stats(stream=False)
                    val = cpu_percent_from_stats(stats)
                    if val is not None:
                        agg += float(val)
                        have_any = True
                except Exception:
                    continue

            out[name] = round(agg, 3) if have_any else None
        return out


def build_header(container_names: List[str]) -> List[str]:
    base = [
        "ts_iso",
        "elapsed_s",
        "window_seconds",
        "delivered_in_window",
        "sent_messages",       # novo campo
        "successful",
        "failed",
        "total_messages",
        "failure_rate",
        "throughput_total",
        "avg_latency_ms_last10",
        "delivery_rate",       # calculado
    ]
    cpu_cols = [f"cpu_{name}" for name in container_names]
    return base + cpu_cols


def safe_ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    try:
        if a is None or b in (None, 0):
            return None
        return (float(a) / float(b)) * 100.0
    except Exception:
        return None


def main():
    ap = argparse.ArgumentParser(
        description="Coleta taxa de entrega usando receiver + sender e CPU de containers."
    )
    ap.add_argument("--receiver-url", default="http://localhost:5001",
                    help="URL do receiver (default: http://localhost:5001)")
    ap.add_argument("--sender-url", default="http://localhost:5000",
                    help="URL do sender (default: http://localhost:5000)")
    ap.add_argument("--window", type=int, default=60,
                    help="Janela (s) para delivered_in_window (default: 60)")
    ap.add_argument("--interval", type=float, default=2.0,
                    help="Intervalo entre amostras em segundos (default: 2.0)")
    ap.add_argument("--duration", type=float, default=300.0,
                    help="Duração total da coleta (default: 300)")
    ap.add_argument("--containers", nargs="+", default=["middleware1"],
                    help="Containers para medir CPU")
    ap.add_argument("--out", default="results_middleware1.csv",
                    help="CSV de saída")
    ap.add_argument("--verbose", action="store_true",
                    help="Logs detalhados")
    args = ap.parse_args()

    header = build_header(args.containers)
    cpu_reader = DockerCPUReader(args.containers, verbose=args.verbose)

    start = time.time()
    next_tick = start
    samples = 0

    with open(args.out, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)

        try:
            while True:
                now = time.time()
                if args.duration > 0 and (now - start) >= args.duration:
                    break

                try:
                    recv = get_metrics(args.receiver_url, args.window)
                except Exception as e:
                    recv = {}
                    print(f"[WARN] Erro lendo receiver: {e}")

                try:
                    send = get_metrics(args.sender_url)
                except Exception as e:
                    send = {}
                    print(f"[WARN] Erro lendo sender: {e}")

                cpu_map = cpu_reader.read_all()

                sent_messages = send.get("total_messages") or send.get("sent_messages")
                delivery_rate = safe_ratio(recv.get("successful"), sent_messages)
                if delivery_rate is not None:
                    delivery_rate = round(delivery_rate, 3)

                row = [
                    iso_now(),
                    round(now - start, 3),
                    args.window,
                    recv.get("delivered_in_window"),
                    sent_messages,
                    recv.get("successful"),
                    recv.get("failed"),
                    recv.get("total_messages"),
                    recv.get("failure_rate"),
                    recv.get("throughput_total"),
                    recv.get("avg_latency_ms_last10"),
                    delivery_rate,
                ]
                for name in args.containers:
                    val = cpu_map.get(name)
                    row.append("" if val is None else round(val, 3))

                w.writerow(row)
                f.flush()
                samples += 1

                next_tick += args.interval
                time.sleep(max(0.0, next_tick - time.time()))

        except KeyboardInterrupt:
            print("\n[INFO] Coleta interrompida pelo usuário.")

    print(f"[OK] {samples} amostras salvas em '{args.out}'.")


if __name__ == "__main__":
    main()
