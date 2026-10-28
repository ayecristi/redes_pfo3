"""PFO 3 - Cliente de consola interactivo (socket TCP)."""
import argparse
import getpass
import json
import socket
import struct
import sys

HOST = '127.0.0.1'
PORT = 65432
TIMEOUT_CONEXION = 5.0
TIMEOUT_RESPUESTA = 15.0
TAMANO_MAX_MENSAJE = 1024 * 1024

# --- Protocolo: 4 bytes de longitud (big-endian) + JSON UTF-8 ---
ENCABEZADO = struct.Struct('!I')


class ConexionCerrada(Exception):
    """El servidor cerró la conexión."""


class ErrorProtocolo(Exception):
    """Mensaje del servidor mal formado."""


def _recibir_exacto(sock, cantidad):
    buffer = bytearray()
    while len(buffer) < cantidad:
        trozo = sock.recv(cantidad - len(buffer))
        if not trozo:
            raise ConexionCerrada()
        buffer.extend(trozo)
    return bytes(buffer)


def enviar_json(sock, objeto):
    datos = json.dumps(objeto).encode('utf-8')
    if len(datos) > TAMANO_MAX_MENSAJE:
        raise ErrorProtocolo('mensaje demasiado grande')
    sock.sendall(ENCABEZADO.pack(len(datos)) + datos)


def recibir_json(sock):
    (longitud,) = ENCABEZADO.unpack(_recibir_exacto(sock, ENCABEZADO.size))
    if longitud > TAMANO_MAX_MENSAJE:
        raise ErrorProtocolo('respuesta demasiado grande')
    try:
        return json.loads(_recibir_exacto(sock, longitud).decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ErrorProtocolo('respuesta con formato inválido') from None


def conectar(host, puerto):
    sock = socket.create_connection((host, puerto), timeout=TIMEOUT_CONEXION)
    sock.settimeout(TIMEOUT_RESPUESTA)  # evita quedar bloqueado si el servidor no responde
    return sock


def enviar_peticion(client_socket, accion, datos=None):
    payload = {"accion": accion, "datos": datos or {}}
    enviar_json(client_socket, payload)
    return recibir_json(client_socket)


def main():
    parser = argparse.ArgumentParser(description="PFO 3 - cliente de tareas")
    parser.add_argument('--host', default=HOST)
    parser.add_argument('--puerto', type=int, default=PORT)
    args = parser.parse_args()

    try:
        client_socket = conectar(args.host, args.puerto)
        print("Conectado al servidor distribuido exitosamente.")
    except OSError:
        print("Error: No se pudo conectar. ¿Está el servidor encendido?")
        sys.exit(1)

    token = None  # el servidor identifica al usuario por este token, no por un id enviado por el cliente
    try:
        while True:
            print("\n--- Sistema de Gestión de Tareas (Socket TCP) ---")
            res = None  # respuesta de esta iteración (evita usar una vieja)
            try:
                # MENÚ CUANDO NO HAY SESIÓN INICIADA
                if not token:
                    print("1. Registrarse")
                    print("2. Iniciar Sesión")
                    print("3. Salir")
                    opcion = input("Elige una opción: ")

                    if opcion == '1':
                        usr = input("Nuevo usuario: ")
                        pwd = getpass.getpass("Nueva contraseña: ")
                        res = enviar_peticion(client_socket, 'registro',
                                              {"usuario": usr, "password": pwd})
                        print(f"Respuesta: {res['mensaje']}")
                    elif opcion == '2':
                        usr = input("Usuario: ")
                        pwd = getpass.getpass("Contraseña: ")
                        res = enviar_peticion(client_socket, 'login',
                                              {"usuario": usr, "password": pwd})
                        print(f"Respuesta: {res['mensaje']}")
                        if res['status'] == 'success':
                            token = res['token']
                    elif opcion == '3':
                        print("Desconectando...")
                        break
                    else:
                        print("Opción inválida.")

                # MENÚ CUANDO SÍ HAY SESIÓN INICIADA
                else:
                    print("1. Crear Tarea")
                    print("2. Ver Mis Tareas")
                    print("3. Cerrar Sesión")
                    print("4. Salir")
                    opcion = input("Elige una opción: ")

                    if opcion == '1':
                        desc = input("Descripción de la tarea: ")
                        res = enviar_peticion(client_socket, 'crear_tarea',
                                              {"token": token, "descripcion": desc})
                        print(f"Respuesta: {res['mensaje']}")
                    elif opcion == '2':
                        res = enviar_peticion(client_socket, 'ver_tareas', {"token": token})
                        if res['status'] == 'success':
                            print("\nTus tareas:")
                            if not res['tareas']:
                                print("- No tenés tareas pendientes.")
                            for t in res['tareas']:
                                print(f"- [{t['estado'].upper()}] {t['descripcion']} (ID: {t['id']})")
                        else:
                            print(f"Error al obtener tareas: {res['mensaje']}")
                    elif opcion == '3':
                        enviar_peticion(client_socket, 'logout', {"token": token})
                        token = None
                        print("Sesión cerrada correctamente.")
                    elif opcion == '4':
                        print("Desconectando...")
                        break
                    else:
                        print("Opción inválida.")

                if res and res.get('sesion_invalida'):
                    token = None
                    print("Tu sesión expiró: volvé a iniciar sesión.")

            except (ConexionCerrada, ErrorProtocolo, socket.timeout, OSError) as exc:
                print(f"Error de comunicación con el servidor: {exc or type(exc).__name__}")
                client_socket.close()
                try:
                    client_socket = conectar(args.host, args.puerto)
                    print("Reconectado. Tu sesión sigue activa; revisá con 'Ver Mis Tareas' "
                          "antes de repetir la operación.")
                except OSError:
                    print("No se pudo reconectar. Saliendo.")
                    break
    finally:
        client_socket.close()


if __name__ == "__main__":
    main()
