#!/usr/bin/env python3
"""
Étape 6 — Simulateur de flux de données (socket TCP)
====================================================

Lit le fichier de ventes ligne par ligne et l'envoie vers un socket TCP
(port 9999 par défaut) pour simuler un flux temps réel.
C'est ce flux que Spark Streaming va consommer (Étape 7).

Usage
-----
    python streaming/simulateur.py [fichier_csv] [host] [port] [delai]

Exemples
--------
    python streaming/simulateur.py data/ecommerce_sales_34500.csv
    python streaming/simulateur.py data/ecommerce_sales_34500.csv localhost 9999 0.05

Le simulateur attend qu'un client (Spark Streaming) se connecte, puis
envoie une ligne toutes les `delai` secondes (0.1s par défaut).
"""
import socket
import sys
import time


def send_data(file_path, host="localhost", port=9999, delay=0.1):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        # Réutiliser l'adresse pour éviter "Address already in use" au redémarrage
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.listen(1)
        print(f"[simulateur] En attente de connexion sur {host}:{port} ...")

        conn, addr = s.accept()
        with conn:
            print(f"[simulateur] Connecté par {addr}")
            sent = 0
            with open(file_path, "r", encoding="utf-8") as f:
                # Sauter l'en-tête du CSV
                header = f.readline()
                print(f"[simulateur] En-tête ignoré : {header.strip()}")

                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        conn.sendall((line + "\n").encode("utf-8"))
                    except (BrokenPipeError, ConnectionResetError):
                        print("[simulateur] Client déconnecté. Arrêt.")
                        return
                    sent += 1
                    if sent % 50 == 0:
                        print(f"[simulateur] {sent} lignes envoyées...")
                    time.sleep(delay)

            print(f"[simulateur] Terminé. {sent} lignes envoyées au total.")


if __name__ == "__main__":
    file_path = sys.argv[1] if len(sys.argv) > 1 else "data/ecommerce_sales_34500.csv"
    host = sys.argv[2] if len(sys.argv) > 2 else "localhost"
    port = int(sys.argv[3]) if len(sys.argv) > 3 else 9999
    delay = float(sys.argv[4]) if len(sys.argv) > 4 else 0.1

    try:
        send_data(file_path, host, port, delay)
    except KeyboardInterrupt:
        print("\n[simulateur] Interrompu par l'utilisateur.")
