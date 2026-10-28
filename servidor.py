"""PFO 3 - Servidor TCP multihilo de gestión de tareas.

Diseño de concurrencia
----------------------
* Hilo principal    : acepta conexiones (accept con timeout para poder apagarse con Ctrl+C).
* Un hilo liviano   : por conexión; solo lee mensajes y escribe respuestas.
* ThreadPoolExecutor: ejecuta las peticiones. Así una conexión inactiva NO ocupa un worker
                      y el pool limita cuánto trabajo se procesa a la vez.

Protocolo
---------
Cada mensaje = 4 bytes (big-endian) con la longitud + cuerpo JSON en UTF-8.
(TCP es un flujo de bytes: sin esta delimitación los mensajes se cortan o se pegan.)
"""
import argparse
import contextlib
import json
import logging
import os
import secrets
import signal
import socket
import sqlite3
import struct
import threading
import time
from concurrent.futures import CancelledError, ThreadPoolExecutor

from werkzeug.security import check_password_hash, generate_password_hash

# NOTA ARQUITECTURA: Para facilitar la ejecución local de este prototipo,
# se utiliza SQLite. En el entorno productivo (según diagrama),
# esta conexión se realiza mediante psycopg2 hacia PostgreSQL.

# --- Configuración (se puede cambiar por variables de entorno o argumentos) ---
HOST = os.getenv('PFO3_HOST', '127.0.0.1')
PORT = int(os.getenv('PFO3_PORT', '65432'))
MAX_WORKERS = int(os.getenv('PFO3_WORKERS', '10'))   # hilos que ejecutan peticiones
MAX_CONEXIONES = 200          # conexiones simultáneas (cada una usa un hilo liviano)
TIMEOUT_INACTIVIDAD = 60.0    # segundos sin mensajes antes de cerrar la conexión
DURACION_SESION = 3600        # segundos de validez del token de sesión
TAMANO_MAX_MENSAJE = 1024 * 1024
DB_FILE = os.getenv('PFO3_DB', 'database.db')

log = logging.getLogger('servidor')


# ============================================================================
# Protocolo: mensajes delimitados por longitud
# ============================================================================
ENCABEZADO = struct.Struct('!I')


class ConexionCerrada(Exception):
    """El cliente cerró la conexión."""


class ErrorProtocolo(Exception):
    """Error irrecuperable de delimitación (p. ej. longitud absurda): hay que cortar."""


class ErrorFormato(Exception):
    """El mensaje llegó completo pero no es JSON válido: se puede responder y seguir."""


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
        raise ErrorProtocolo(f'longitud declarada excesiva ({longitud} bytes)')
    cuerpo = _recibir_exacto(sock, longitud)
    try:
        return json.loads(cuerpo.decode('utf-8'))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise ErrorFormato() from None


# ============================================================================
# Base de datos
# ============================================================================
@contextlib.contextmanager
def conectar_db():
    """Una conexión por operación: segura entre hilos y entre procesos.

    SQLite serializa las escrituras por sí mismo (timeout=10 espera el turno), por eso
    ya no hace falta un lock global; además ese lock solo valía dentro de un proceso.
    """
    conn = sqlite3.connect(DB_FILE, timeout=10)
    conn.execute('PRAGMA foreign_keys = ON')   # SQLite no aplica las FK si no se activa
    try:
        with conn:  # commit si todo salió bien, rollback si hubo excepción
            yield conn
    finally:
        conn.close()  # `with conn` NO cierra la conexión, por eso el close explícito


def init_db():
    """Inicializa la base de datos (En la arquitectura real, esto es PostgreSQL)"""
    with conectar_db() as conn:
        conn.execute('PRAGMA journal_mode = WAL')  # lecturas concurrentes con una escritura
        conn.execute('''
            CREATE TABLE IF NOT EXISTS usuarios (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )
        ''')
        conn.execute('''
            CREATE TABLE IF NOT EXISTS tareas (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                usuario_id INTEGER NOT NULL,
                descripcion TEXT NOT NULL,
                estado TEXT DEFAULT 'pendiente',
                FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
            )
        ''')
        # Las sesiones viven en la base compartida (no en la memoria de un servidor):
        # si hay varias instancias detrás de un balanceador, cualquiera puede validar el token.
        conn.execute('''
            CREATE TABLE IF NOT EXISTS sesiones (
                token TEXT PRIMARY KEY,
                usuario_id INTEGER NOT NULL,
                expira_en REAL NOT NULL,
                FOREIGN KEY (usuario_id) REFERENCES usuarios (id)
            )
        ''')


# ============================================================================
# Lógica de negocio
# ============================================================================
class ErrorPeticion(Exception):
    """Error atribuible al cliente (datos inválidos, sin sesión...)."""

    def __init__(self, mensaje, sesion_invalida=False):
        super().__init__(mensaje)
        self.sesion_invalida = sesion_invalida


def _ok(mensaje, **extra):
    return {"status": "success", "mensaje": mensaje, **extra}


def _error(mensaje, **extra):
    return {"status": "error", "mensaje": mensaje, **extra}


def _texto(datos, clave, maximo):
    valor = datos.get(clave)
    if not isinstance(valor, str) or not valor.strip():
        raise ErrorPeticion(f"Falta el campo '{clave}'")
    valor = valor.strip()
    if len(valor) > maximo:
        raise ErrorPeticion(f"'{clave}' no puede superar {maximo} caracteres")
    return valor


def _password(datos):
    password = datos.get('password')
    if not isinstance(password, str) or not password:
        raise ErrorPeticion("Falta el campo 'password'")
    if len(password) > 128:  # evita gastar CPU hasheando textos gigantes
        raise ErrorPeticion("'password' no puede superar 128 caracteres")
    return password


def _autenticar(datos):
    """Devuelve el usuario_id dueño del token. El servidor NUNCA confía en un id del cliente."""
    token = datos.get('token')
    if not isinstance(token, str) or not token:
        raise ErrorPeticion("Sesión inválida o expirada. Iniciá sesión nuevamente.", True)
    with conectar_db() as conn:
        fila = conn.execute('SELECT usuario_id, expira_en FROM sesiones WHERE token = ?',
                            (token,)).fetchone()
        if fila is None or fila[1] < time.time():
            if fila is not None:
                conn.execute('DELETE FROM sesiones WHERE token = ?', (token,))
            raise ErrorPeticion("Sesión inválida o expirada. Iniciá sesión nuevamente.", True)
    return fila[0]


# Se verifica contra este hash cuando el usuario no existe, para que el login tarde lo mismo
# exista o no la cuenta (evita descubrir usuarios midiendo tiempos de respuesta).
_HASH_FALSO = generate_password_hash('relleno')


def manejar_registro(datos):
    usuario = _texto(datos, 'usuario', 50)
    password = _password(datos)
    hashed_pw = generate_password_hash(password)  # costoso: se hace fuera de la transacción
    try:
        with conectar_db() as conn:
            conn.execute('INSERT INTO usuarios (usuario, password_hash) VALUES (?, ?)',
                         (usuario, hashed_pw))
    except sqlite3.IntegrityError:
        return _error("El usuario ya existe")
    return _ok("Usuario registrado exitosamente")


def manejar_login(datos):
    usuario = _texto(datos, 'usuario', 50)
    password = _password(datos)
    with conectar_db() as conn:
        fila = conn.execute('SELECT id, password_hash FROM usuarios WHERE usuario = ?',
                            (usuario,)).fetchone()
    if fila is None:
        check_password_hash(_HASH_FALSO, password)
        return _error("Credenciales inválidas")
    if not check_password_hash(fila[1], password):
        return _error("Credenciales inválidas")

    token = secrets.token_urlsafe(32)
    ahora = time.time()
    with conectar_db() as conn:
        conn.execute('DELETE FROM sesiones WHERE expira_en < ?', (ahora,))  # limpieza
        conn.execute('INSERT INTO sesiones (token, usuario_id, expira_en) VALUES (?, ?, ?)',
                     (token, fila[0], ahora + DURACION_SESION))
    return _ok("Login exitoso", token=token)


def manejar_logout(datos):
    _autenticar(datos)
    with conectar_db() as conn:
        conn.execute('DELETE FROM sesiones WHERE token = ?', (datos['token'],))
    return _ok("Sesión cerrada")


def manejar_crear_tarea(datos):
    # NOTA ARQUITECTURA: Esto idealmente publicaría un mensaje en RabbitMQ
    usuario_id = _autenticar(datos)
    descripcion = _texto(datos, 'descripcion', 500)
    with conectar_db() as conn:
        cur = conn.execute('INSERT INTO tareas (usuario_id, descripcion) VALUES (?, ?)',
                           (usuario_id, descripcion))
        tarea_id = cur.lastrowid
    return _ok("Tarea creada exitosamente", id=tarea_id)


def manejar_ver_tareas(datos):
    usuario_id = _autenticar(datos)
    with conectar_db() as conn:
        filas = conn.execute('SELECT id, descripcion, estado FROM tareas '
                             'WHERE usuario_id = ? ORDER BY id', (usuario_id,)).fetchall()
    return {"status": "success",
            "tareas": [{"id": f[0], "descripcion": f[1], "estado": f[2]} for f in filas]}


ACCIONES = {
    'registro': manejar_registro,
    'login': manejar_login,
    'logout': manejar_logout,
    'crear_tarea': manejar_crear_tarea,
    'ver_tareas': manejar_ver_tareas,
}


def procesar_peticion(payload):
    """Enrutador. Nunca lanza excepciones: siempre devuelve una respuesta al cliente.

    En una arquitectura completa, las tareas de escritura pesadas se enviarían a RabbitMQ acá.
    """
    try:
        if not isinstance(payload, dict):
            return _error("La petición debe ser un objeto JSON")
        datos = payload.get('datos', {})
        if not isinstance(datos, dict):
            return _error("'datos' debe ser un objeto JSON")
        manejador = ACCIONES.get(payload.get('accion'))
        if manejador is None:
            return _error("Acción no reconocida")
        return manejador(datos)
    except ErrorPeticion as exc:
        extra = {"sesion_invalida": True} if exc.sesion_invalida else {}
        return _error(str(exc), **extra)
    except Exception:  # noqa: BLE001 - un bug no debe dejar al cliente sin respuesta
        log.exception("Error no controlado procesando la petición")
        return _error("Error interno del servidor")


# ============================================================================
# Servidor
# ============================================================================
def handle_client(conn, addr, executor, cupos):
    """Hilo liviano por conexión: lee, delega la ejecución al pool y responde."""
    print(f"[NUEVA CONEXIÓN] Cliente conectado desde {addr}")
    conn.settimeout(TIMEOUT_INACTIVIDAD)
    try:
        while True:
            try:
                payload = recibir_json(conn)
            except ErrorFormato:
                enviar_json(conn, _error("Formato JSON inválido"))
                continue  # el mensaje llegó entero: la conexión sigue sincronizada
            print(f"[RECV] de {addr}: {payload.get('accion') if isinstance(payload, dict) else '?'}")
            try:
                respuesta = executor.submit(procesar_peticion, payload).result()
            except (RuntimeError, CancelledError):  # el pool se está apagando
                enviar_json(conn, _error("Servidor apagándose"))
                break
            enviar_json(conn, respuesta)
    except ConexionCerrada:
        pass
    except socket.timeout:
        print(f"[INACTIVIDAD] Cerrando conexión de {addr}")
    except ErrorProtocolo as exc:
        print(f"[ERROR PROTOCOLO] {addr}: {exc}")
        with contextlib.suppress(OSError, ErrorProtocolo):
            enviar_json(conn, _error("Error de protocolo"))
    except OSError:  # ConnectionResetError, BrokenPipeError...
        print(f"[DESCONEXIÓN ABRUPTA] {addr}")
    finally:
        conn.close()
        cupos.release()
        print(f"[DESCONECTADO] {addr}")


def iniciar_servidor(host=HOST, port=PORT, workers=MAX_WORKERS):
    init_db()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    # El socket "respira" cada 1 segundo para poder atender Ctrl+C
    server.settimeout(1.0)
    server.bind((host, port))
    server.listen(128)

    print(f"[INICIANDO] Servidor TCP escuchando en {host}:{port}")
    print(f"[ARQUITECTURA] ThreadPoolExecutor con {workers} workers "
          f"(hasta {MAX_CONEXIONES} conexiones simultáneas)")

    executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='worker')
    cupos = threading.BoundedSemaphore(MAX_CONEXIONES)
    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue  # no se conectó nadie en este segundo
            if not cupos.acquire(blocking=False):
                with contextlib.suppress(OSError, ErrorProtocolo):
                    enviar_json(conn, _error("Servidor ocupado, intentá más tarde"))
                conn.close()
                continue
            # daemon=True: al apagar, las conexiones abiertas no bloquean la salida del proceso
            threading.Thread(target=handle_client, args=(conn, addr, executor, cupos),
                             daemon=True).start()
    except KeyboardInterrupt:
        print("\n[APAGANDO] Cerrando el servidor...")
    finally:
        server.close()
        executor.shutdown(wait=True, cancel_futures=True)  # termina las peticiones en curso
        print("[APAGADO] Servidor cerrado correctamente.")


def _al_recibir_sigterm(*_):
    raise KeyboardInterrupt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PFO 3 - servidor de tareas por sockets")
    parser.add_argument('--host', default=HOST)
    parser.add_argument('--puerto', type=int, default=PORT)
    parser.add_argument('--workers', type=int, default=MAX_WORKERS)
    parser.add_argument('--db', default=DB_FILE, help="ruta de la base SQLite")
    args = parser.parse_args()
    DB_FILE = args.db
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    signal.signal(signal.SIGTERM, _al_recibir_sigterm)
    iniciar_servidor(args.host, args.puerto, args.workers)
