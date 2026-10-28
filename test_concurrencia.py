"""Prueba de concurrencia y seguridad (reproduce la captura del README).

Uso (con el servidor corriendo):
    python test_concurrencia.py                 # 20 clientes simultáneos
    python test_concurrencia.py --clientes 50
"""
import argparse
import sys
import threading
import time
import uuid

from cliente import HOST, PORT, conectar, enviar_peticion

resultados = {}
fallos = []


def cliente_completo(n, host, puerto, sufijo):
    """Registro -> login -> 3 tareas -> listado, todo por una misma conexión."""
    usuario, password = f"test_{sufijo}_{n}", "clave-de-prueba"
    try:
        sock = conectar(host, puerto)
        try:
            r = enviar_peticion(sock, 'registro', {"usuario": usuario, "password": password})
            assert r['status'] == 'success', r
            r = enviar_peticion(sock, 'login', {"usuario": usuario, "password": password})
            assert r['status'] == 'success', r
            token = r['token']
            for i in range(3):
                r = enviar_peticion(sock, 'crear_tarea',
                                    {"token": token, "descripcion": f"tarea {i} de {usuario}"})
                assert r['status'] == 'success', r
            r = enviar_peticion(sock, 'ver_tareas', {"token": token})
            assert len(r['tareas']) == 3, r                       # ni más ni menos: solo las suyas
            assert all(usuario in t['descripcion'] for t in r['tareas']), r
            resultados[n] = (usuario, token)
        finally:
            sock.close()
    except Exception as exc:  # noqa: BLE001
        fallos.append(f"cliente {n}: {exc!r}")


def fase_concurrencia(cantidad, host, puerto):
    print(f"\n[1] {cantidad} clientes simultáneos (registro, login, 3 tareas, listado)")
    sufijo = uuid.uuid4().hex[:6]
    inicio = time.perf_counter()
    hilos = [threading.Thread(target=cliente_completo, args=(i, host, puerto, sufijo))
             for i in range(cantidad)]
    [h.start() for h in hilos]
    [h.join() for h in hilos]
    print(f"    completados: {len(resultados)}/{cantidad} en {time.perf_counter() - inicio:.2f}s")


def fase_seguridad(host, puerto):
    print("\n[2] Aislamiento entre usuarios y control de sesión")
    sock = conectar(host, puerto)
    try:
        r = enviar_peticion(sock, 'ver_tareas', {"usuario_id": 1})
        assert r['status'] == 'error', "sin token NO debe poder ver tareas"
        r = enviar_peticion(sock, 'ver_tareas', {"token": "token-inventado"})
        assert r['status'] == 'error' and r.get('sesion_invalida'), "token falso debe rechazarse"
        r = enviar_peticion(sock, 'crear_tarea', {"usuario_id": 1, "descripcion": "intruso"})
        assert r['status'] == 'error', "sin token NO debe poder crear tareas"
        (_, token_a), (_, token_b) = resultados[0], resultados[1]
        tareas_a = {t['id'] for t in enviar_peticion(sock, 'ver_tareas', {"token": token_a})['tareas']}
        tareas_b = {t['id'] for t in enviar_peticion(sock, 'ver_tareas', {"token": token_b})['tareas']}
        assert tareas_a.isdisjoint(tareas_b), "los usuarios ven tareas ajenas"
        enviar_peticion(sock, 'logout', {"token": token_a})
        r = enviar_peticion(sock, 'ver_tareas', {"token": token_a})
        assert r['status'] == 'error', "el token debe invalidarse tras logout"
    finally:
        sock.close()
    print("    OK")


def fase_conexiones_inactivas(host, puerto, cantidad):
    print(f"\n[3] {cantidad} conexiones abiertas e inactivas; un cliente nuevo debe ser atendido")
    inactivas = [conectar(host, puerto) for _ in range(cantidad)]
    try:
        sock = conectar(host, puerto)
        try:
            inicio = time.perf_counter()
            r = enviar_peticion(sock, 'login', {"usuario": "no-existe", "password": "x"})
            assert r['status'] == 'error'
            print(f"    respondido en {time.perf_counter() - inicio:.2f}s (no quedó bloqueado)")
        finally:
            sock.close()
    finally:
        for s in inactivas:
            s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', default=HOST)
    parser.add_argument('--puerto', type=int, default=PORT)
    parser.add_argument('--clientes', type=int, default=20)
    parser.add_argument('--inactivas', type=int, default=15,
                        help="conexiones inactivas de la fase 3 (conviene que superen --workers)")
    args = parser.parse_args()

    fase_concurrencia(args.clientes, args.host, args.puerto)
    if fallos:
        print("\nFALLOS:", *fallos, sep="\n  ")
        sys.exit(1)
    try:
        if len(resultados) >= 2:
            fase_seguridad(args.host, args.puerto)
        fase_conexiones_inactivas(args.host, args.puerto, args.inactivas)
    except (AssertionError, OSError) as exc:
        print(f"\nFALLÓ: {exc!r}")
        sys.exit(1)
    print("\nTodas las pruebas pasaron.")


if __name__ == "__main__":
    main()
