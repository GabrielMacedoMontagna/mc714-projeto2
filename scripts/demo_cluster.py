#!/usr/bin/env python3
"""Script de demonstração ao vivo para o cluster Docker Compose (5 nós).

Executa e exibe no terminal:
1. Cenário Normal: Exclusão Mútua (Ricart-Agrawala) e Escrita/Leitura por Quorum;
2. Cenário de Falha Injetada: Crash-Stop do Nó 5 e isolamento do Nó 4 por partição
   ({1,2,3} | {4} | {5}), com medição do tempo de detecção pelo Heartbeat;
3. Recuperação e Consequência Esperada: escrita aceita na maioria {1,2,3}, escrita
   rejeitada no Nó 4 isolado e reparo automático das réplicas defasadas (Read-Repair).
"""

from __future__ import annotations

import argparse
import asyncio
import time

from mc714_p2.node.config import parse_peers_spec
from mc714_p2.rpc.client import PeerRpcClient
from mc714_p2.rpc.proto_gen import cluster_pb2

DEFAULT_COMPOSE_PEERS = (
    "1=127.0.0.1:50051,2=127.0.0.1:50052,3=127.0.0.1:50053,"
    "4=127.0.0.1:50054,5=127.0.0.1:50055"
)


async def _wait_until_suspected(
    client: PeerRpcClient,
    observer_id: int,
    expected: set[int],
    max_wait_sec: float = 3.0,
    poll_sec: float = 0.02,
) -> tuple[float, list[int], list[int]]:
    """Consulta o observador até que todos os nós esperados estejam suspeitos."""
    t0 = time.perf_counter()
    while True:
        st = await client.get_node_status(peer_id=observer_id, rpc_timeout_sec=2.0)
        suspected = set(st.suspected_peers)
        elapsed = time.perf_counter() - t0
        if expected <= suspected or elapsed >= max_wait_sec:
            return elapsed * 1000.0, sorted(suspected), list(st.active_peers)
        await asyncio.sleep(poll_sec)


async def run_demo(peers_spec: str) -> None:
    endpoints = parse_peers_spec(peers_spec)
    client = PeerRpcClient(endpoints)
    try:
        print("=== [1/4] Verificando status inicial dos 5 nós do cluster ===")
        for nid in sorted(endpoints):
            await client.configure_faults(
                peer_id=nid,
                config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
                rpc_timeout_sec=2.0,
            )
        await asyncio.sleep(0.30)
        for nid in sorted(endpoints):
            st = await client.get_node_status(peer_id=nid, rpc_timeout_sec=2.0)
            print(
                f"  -> Nó {st.node_id}: Lamport={st.lamport_clock}, "
                f"Mutex={st.mutex_state}, Peers Ativos={list(st.active_peers)}"
            )

        print(
            "\n=== [2/4] Cenário Normal: Disputa Concorrente pela Seção Crítica "
            "(Ricart-Agrawala) e Escrita Coordenada por Quorum (R=3, W=3) ==="
        )
        n_nodes = len(endpoints)
        solo = await client.client_execute_cs(
            peer_id=1,
            resource_tag="recurso-compartilhado",
            hold_ms=10.0,
            timeout_ms=3000.0,
            rpc_timeout_sec=4.0,
        )
        print(
            f"  [Sem contenção] Nó 1 entrou sozinho na SC | "
            f"Msgs={solo.protocol_messages_used} (teórico 2(N-1)={2 * (n_nodes - 1)})"
        )
        print("  [Com contenção] Nós 1, 2 e 3 pedem a SC ao mesmo tempo:")
        cs_replies = await asyncio.gather(
            client.client_execute_cs(
                peer_id=1,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-1",
                rpc_timeout_sec=4.0,
            ),
            client.client_execute_cs(
                peer_id=2,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-2",
                rpc_timeout_sec=4.0,
            ),
            client.client_execute_cs(
                peer_id=3,
                resource_tag="recurso-compartilhado",
                hold_ms=25.0,
                timeout_ms=3000.0,
                optional_quorum_key="config:lider",
                optional_quorum_value="atualizado-por-no-3",
                rpc_timeout_sec=4.0,
            ),
        )
        for rep in sorted(cs_replies, key=lambda r: r.enter_lamport_ts):
            msgs = rep.protocol_messages_used
            print(
                f"  [SC OK] Nó {rep.node_id} | Req={rep.request_lamport_ts} -> "
                f"Intervalo=[{rep.enter_lamport_ts}, {rep.exit_lamport_ts}] | "
                f"Espera={rep.wait_latency_ms:.2f}ms | Msgs={msgs}"
            )
        print(
            "  (Intervalos de Lamport disjuntos = exclusão mútua. Sob contenção, "
            "cada nó também responde aos pedidos dos demais concorrentes.)"
        )

        print(
            "\n=== [3/4] Cenário de Falha Injetada: Crash-Stop no Nó 5 e "
            "Partição de Rede no Nó 4 ==="
        )
        await client.configure_faults(
            peer_id=5,
            config_msg=cluster_pb2.FaultConfigMsg(set_crashed=True, crashed=True),
            rpc_timeout_sec=2.0,
        )
        await client.configure_faults(
            peer_id=4,
            config_msg=cluster_pb2.FaultConfigMsg(
                set_partition=True,
                partitioned_peers=[1, 2, 3, 5],
            ),
            rpc_timeout_sec=2.0,
        )
        for maj in (1, 2, 3):
            await client.configure_faults(
                peer_id=maj,
                config_msg=cluster_pb2.FaultConfigMsg(
                    set_partition=True,
                    partitioned_peers=[4],
                ),
                rpc_timeout_sec=2.0,
            )

        detect_ms, suspected, active = await _wait_until_suspected(
            client, observer_id=1, expected={4, 5}
        )
        print(
            f"  -> Detector de Falhas no Nó 1 suspeitou de {suspected} "
            f"em ~{detect_ms:.0f} ms após a injeção (Ativos restantes: {active})"
        )

        w_maj = await client.client_quorum_write(
            peer_id=1,
            key="config:lider",
            value="valor-pos-falha-quorum-majoritario",
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Escrita na partição majoritária {{1,2,3}}: "
            f"sucesso={w_maj.success}, v={w_maj.version}, "
            f"ACKs={w_maj.write_phase_acks} (quórum W=3)"
        )

        w_min = await client.client_quorum_write(
            peer_id=4,
            key="config:lider",
            value="tentativa-invalida-particao-minoritaria",
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Escrita na partição minoritária isolada {{4}}: "
            f"sucesso={w_min.success} (Bloqueada com segurança: {w_min.error_message})"
        )

        print("\n=== [4/4] Recuperação dos Nós 4 e 5 + Read-Repair Automático ===")
        for nid in sorted(endpoints):
            await client.configure_faults(
                peer_id=nid,
                config_msg=cluster_pb2.FaultConfigMsg(reset_all=True),
                rpc_timeout_sec=2.0,
            )
        await asyncio.sleep(0.25)

        r_repair = await client.client_quorum_read(
            peer_id=1,
            key="config:lider",
            enable_read_repair=True,
            rpc_timeout_sec=3.0,
        )
        print(
            f"  -> Leitura por Quórum: valor='{r_repair.value}', "
            f"v={r_repair.version}, ACKs={r_repair.acks_received}, "
            f"Réplicas Reparadas={list(r_repair.repaired_nodes)}"
        )
    finally:
        await client.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Demonstração ao vivo do cluster MC714."
    )
    parser.add_argument("--peers", default=DEFAULT_COMPOSE_PEERS)
    args = parser.parse_args()
    asyncio.run(run_demo(args.peers))


if __name__ == "__main__":
    main()
