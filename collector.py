#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# collector.py
#
# Coleta métricas do receiver (mensagens/s) e CPU (%) do container do middleware1,
# salva CSV e plota um gráfico com dois eixos Y.
#
# Dep.: pip install requests docker matplotlib

import argparse
import csv
import time
from datetime import datetime, timezone
import requests

try:
    import docker
except Exception:
    docker = None

SENDER_URL   = "http://localhost:5000"
RECEIVER_URL = "http://localhost:5001"

def iso_now():
    return datetime.now(timezone.utc).isoformat()

def http_get_json(url, timeout=3.0):
    return requests.get(url, timeout=timeout).json()

def http_post_json(url, timeout=3.0):
    return requests.post(url, timeout=timeout).json()

def reset_endpoints():
    try:
        http_post_json(f"{RECEIVER_URL}/reset")
    except Exception as e:
        print(f"[warn] reset receiver falhou: {e}")
    try:
        requests.get(f"{SENDER_URL}/reset-stats", timeout=3.0)
    except Exception as e:
        print(f"[warn] reset sender falhou: {e}")

def start_scenario(name, seconds, rate=None, mps=None):
    if name == "intermittent":
        r = http_get_json(f"{SENDER_URL}/scenario/intermittent?rate={rate if rate is not None else 0.3}&seconds={seconds}")
    elif name == "complete":
        r = http_get_json(f"{SENDER_URL}/scenario/complete?seconds={seconds}")
    elif name == "overload":
        mps = mps if mps is not None else 1000
        r = http_get_json(f"{SENDER_URL}/scenario/overload?mps={mps}&seconds={seconds}")
    else:
        raise ValueError("scenario inválido")
    return r

def ensure_sender_running():
    try:
        requests.get(f"{SENDER_URL}/start", timeout=3.0)
    except Exception as e:
        print(f"[warn] não consegui startar sender: {e}")

# ---- CPU de container Docker (%)
def get_docker_client():
    if docker is None:
        raise RuntimeError("módulo docker não disponível. Instale com: pip install docker")
    return docker.from_env()

def cpu_percent_from_stats(prev, cur):
    try:
        cpu_delta = cur["cpu_stats"]["cpu_usage"]["total_usage"] - prev["cpu_stats"]["cpu_usage"]["total_usage"]
        system_delta = cur["cpu_stats"]["system_cpu_usage"] - prev["cpu_stats"]["system_cpu_usage"]
        online_cpus = cur["cpu_stats"].get("online_cpus") or cur["cpu_stats"]["cpu_usage"].get("percpu_usage") and len(cur["cpu_stats"]["cpu_usage"]["percpu_usage"]) or 1
        if system_delta > 0 and cpu_delta >= 0:
            return (cpu_delta / system_delta) * online_cpus * 100.0
    except Exception:
        pass
    return 0.0

def sample_container_cpu(container):
    s1 = container.stats(stream=False)
    time.sleep(0.3)
    s2 = container.stats(stream=False)
    return cpu_percent_from_stats(s1, s2)

# ---- Coleta principal
def collect(duration_s=120, csv_path="metrics_run.csv", scenario="intermittent", rate=0.3, mps=1000, container_name="middleware1"):
    reset_endpoints()
    ensure_sender_running()
    start_scenario(scenario, duration_s, rate=rate, mps=mps)

    # LOG inicial de configuração
    print(f"[collect] scenario={scenario}, duration={duration_s}s, rate={rate}, mps={mps}, container={container_name}")

    client = get_docker_client()
    mw = client.containers.get(container_name)

    started = time.perf_counter()
    last_elapsed = -1

    with open(csv_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "timestamp","elapsed_s","delivered_per_s",
            "successful_total","failed_total","total_messages",
            "delivery_rate_cum","cpu_middleware1"
        ])

        while True:
            now = time.perf_counter()
            elapsed = int(now - started)
            if elapsed == last_elapsed:
                time.sleep(0.05)
                continue
            last_elapsed = elapsed

            try:
                r = http_get_json(f"{RECEIVER_URL}/metrics?window=1")
                delivered_per_s = r.get("delivered_in_window", 0)
                successful_total = r.get("successful", 0)
                failed_total = r.get("failed", 0)
                total_msgs = r.get("total_messages", successful_total + failed_total)
                delivery_rate_cum = (successful_total / total_msgs) * 100.0 if total_msgs > 0 else 0.0
            except Exception as e:
                print(f"[warn] receiver/metrics erro: {e}")
                delivered_per_s = 0
                successful_total = failed_total = total_msgs = 0
                delivery_rate_cum = 0.0

            try:
                cpu_mw = sample_container_cpu(mw)
            except Exception as e:
                print(f"[warn] docker stats erro: {e}")
                cpu_mw = 0.0

            # LOG por segundo mostrando CPU com alta precisão e demais métricas básicas
            print(f"[t+{elapsed:03d}s] msgs/s={delivered_per_s:.3f}  "
                  f"succ={successful_total}  fail={failed_total}  "
                  f"total={total_msgs}  rate_cum={delivery_rate_cum:.2f}%  "
                  f"CPU={cpu_mw:.6f}%")

            writer.writerow([
                iso_now(), elapsed, delivered_per_s, successful_total, failed_total,
                total_msgs, round(delivery_rate_cum, 2), round(cpu_mw, 5)
            ])
            f.flush()

            if elapsed >= duration_s:
                break

    print(f"[ok] CSV salvo em: {csv_path}")

    # Plot com cores distintas e legenda
    try:
        import matplotlib.pyplot as plt

        xs, msgsps, cpu_series = [], [], []
        with open(csv_path, "r") as f:
            rd = csv.DictReader(f)
            for row in rd:
                xs.append(int(row["elapsed_s"]))
                msgsps.append(float(row["delivered_per_s"]))
                cpu_series.append(float(row["cpu_middleware1"]))

        fig, ax1 = plt.subplots()
        ln1 = ax1.plot(xs, msgsps, 'b-', label="Mensagens/s")  # azul
        ax1.set_xlabel("Tempo (s)")
        ax1.set_ylabel("Mensagens entregues por segundo", color='b')
        ax1.tick_params(axis='y', labelcolor='b')
        ax1.grid(True, which="both", linestyle="--", alpha=0.4)

        ax2 = ax1.twinx()
        ln2 = ax2.plot(xs, cpu_series, 'r-', label="CPU Middleware (%)")  # vermelho
        ax2.set_ylabel("Uso de CPU do Middleware (%)", color='r')
        ax2.tick_params(axis='y', labelcolor='r')

        lns = ln1 + ln2
        labs = [l.get_label() for l in lns]
        ax1.legend(lns, labs, loc='upper left')

        plt.title(f"Mensagens/s (azul) e CPU do Middleware (%) (vermelho) — cenário: {scenario}")
        plt.tight_layout()
        plt.show()
    except Exception as e:
        print(f"[warn] não foi possível plotar: {e}\nInstale matplotlib com: pip install matplotlib")

def main():
    p = argparse.ArgumentParser(description="Coletor de métricas (receiver+CPU middleware) + plot")
    p.add_argument("--duration", type=int, default=120, help="duração em segundos")
    p.add_argument("--csv", type=str, default="metrics_run.csv", help="arquivo CSV de saída")
    p.add_argument("--scenario", choices=["intermittent","complete","overload"], default="intermittent")
    p.add_argument("--rate", type=float, default=0.3, help="taxa de falha intermitente (0..1)")
    p.add_argument("--mps", type=int, default=1000, help="mensagens/seg para overload")
    p.add_argument("--container", type=str, default="middleware1", help="nome do container do middleware")
    args = p.parse_args()

    collect(duration_s=args.duration, csv_path=args.csv, scenario=args.scenario,
            rate=args.rate, mps=args.mps, container_name=args.container)

if __name__ == "__main__":
    main()
