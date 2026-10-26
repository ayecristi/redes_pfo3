import socket
import json
import sys

HOST = '127.0.0.1'
PORT = 65432
BUFFER_SIZE = 4096

def enviar_peticion(client_socket, accion, datos=None):
    if datos is None:
        datos = {}
    payload = {"accion": accion, "datos": datos}
    client_socket.sendall(json.dumps(payload).encode('utf-8'))
    
    respuesta_bytes = client_socket.recv(BUFFER_SIZE)
    return json.loads(respuesta_bytes.decode('utf-8'))

def main():
    try:
        client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_socket.connect((HOST, PORT))
        print("Conectado al servidor distribuido exitosamente.")
    except ConnectionRefusedError:
        print("Error: No se pudo conectar. ¿Está el servidor encendido?")
        sys.exit()

    usuario_id_actual = None

    try:
        while True:
            print("\n--- Sistema de Gestión de Tareas (Socket TCP) ---")
            
            # MENÚ CUANDO NO HAY SESIÓN INICIADA
            if not usuario_id_actual:
                print("1. Registrarse")
                print("2. Iniciar Sesión")
                print("3. Salir")
                opcion = input("Elige una opción: ")

                if opcion == '1':
                    usr = input("Nuevo usuario: ")
                    pwd = input("Nueva contraseña: ")
                    res = enviar_peticion(client_socket, 'registro', {"usuario": usr, "password": pwd})
                    print(f"Respuesta: {res['mensaje']}")

                elif opcion == '2':
                    usr = input("Usuario: ")
                    pwd = input("Contraseña: ")
                    res = enviar_peticion(client_socket, 'login', {"usuario": usr, "password": pwd})
                    print(f"Respuesta: {res['mensaje']}")
                    if res['status'] == 'success':
                        usuario_id_actual = res['usuario_id']

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
                    res = enviar_peticion(client_socket, 'crear_tarea', {"usuario_id": usuario_id_actual, "descripcion": desc})
                    print(f"Respuesta: {res['mensaje']}")

                elif opcion == '2':
                    res = enviar_peticion(client_socket, 'ver_tareas', {"usuario_id": usuario_id_actual})
                    if res['status'] == 'success':
                        print("\nTus tareas:")
                        if not res['tareas']:
                            print("- No tenés tareas pendientes.")
                        for t in res['tareas']:
                            print(f"- [{t['estado'].upper()}] {t['descripcion']} (ID: {t['id']})")
                    else:
                        print("Error al obtener tareas.")

                elif opcion == '3':
                    usuario_id_actual = None
                    print("Sesión cerrada correctamente.")

                elif opcion == '4':
                    print("Desconectando...")
                    break
                else:
                    print("Opción inválida.")
    finally:
        client_socket.close()

if __name__ == "__main__":
    main()